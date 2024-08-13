
import os
import glob
import webdataset as wds
from concurrent.futures import ThreadPoolExecutor
from tqdm import trange, tqdm
import cv2

# def package_videos(file_list, output_folder, shard_id, target_size=500e6):
#     pattern = os.path.join(output_folder, f"{shard_id:03d}-%06d.tar")
#     if shard_id == 0:
#         disable_pbar = False
#     else:
#         disable_pbar = True
    
#     with wds.ShardWriter(pattern, maxcount=1e9, maxsize=target_size, compress=False, verbose=False) as sink:
#         for fpath in tqdm(file_list, total=len(file_list), ncols=60, disable=disable_pbar):
#             video = cv2.VideoCapture(fpath)
#             if not video.isOpened():
#                 print(f"Warning: unable to open {fpath}")
#                 continue

#             frame_width = int(video.get(cv2.CAP_PROP_FRAME_WIDTH))
#             frame_height = int(video.get(cv2.CAP_PROP_FRAME_HEIGHT))
            
#             # 跳过尺寸不符合要求的视频
#             if frame_width < 420 or frame_height < 240:
#                 video.release()
#                 continue

#             # 如果尺寸太大，需要调整尺寸
#             if frame_width > 512 or frame_height > 320:
#                 # 计算新的尺寸
#                 aspect_ratio = frame_width / frame_height
#                 if 512 / aspect_ratio <= 320:
#                     resized_width = 512
#                     resized_height = int(512 / aspect_ratio)
#                 else:
#                     resized_height = 320
#                     resized_width = int(320 * aspect_ratio)
                
#                 # 调整视频尺寸
#                 resized_frames = []
#                 while True:
#                     ret, frame = video.read()
#                     if not ret:
#                         break
#                     resized_frame = cv2.resize(frame, (resized_width, resized_height), interpolation=cv2.INTER_AREA)
#                     resized_frames.append(resized_frame)

#                 # video_data = b"".join([cv2.imencode('.jpg', frame)[1].tobytes() for frame in resized_frames])
#                 video_data = b"".join([cv2.imencode('.jpg', cv2.cvtColor(frame, cv2.COLOR_BGR2RGB))[1].tobytes() for frame in resized_frames])
#             else:
#                 # 如果不需要调整，读取整个视频
                
#                 # video_data = b"".join([cv2.imencode('.jpg', cv2.cvtColor(frame, cv2.COLOR_BGR2RGB))[1].tobytes() for ret, frame in iter(lambda: video.read(), (False, None))])
#                 video_data = b"".join([cv2.imencode('.jpg', cv2.cvtColor(frame, cv2.COLOR_BGR2RGB))[1].tobytes() for ret, frame in iter(lambda: video.read(), (False, None))])

#             # 写入 tar 文件
#             sink.write({
#                 "__key__": os.path.basename(fpath),
#                 "mp4": video_data
#             })

            # video.release()
import av
import os
from tqdm import tqdm
import webdataset as wds
import io


def package_videos(file_list, output_folder, shard_id, target_size=500e6):
    pattern = os.path.join(output_folder, f"{shard_id:03d}-%06d.tar")
    with wds.ShardWriter(pattern, maxcount=1e9, maxsize=target_size, compress=False, verbose=False) as sink:
        for fpath in tqdm(file_list, total=len(file_list), ncols=60, disable=(shard_id != 0)):
            video = av.open(fpath)
            stream = video.streams.video[0]
            aspect_ratio = stream.codec_context.width / stream.codec_context.height

            if stream.codec_context.width < 420 or stream.codec_context.height < 240:
                video.close()
                continue

            resized_width, resized_height = (512, int(512 / aspect_ratio)) if 512 / aspect_ratio <= 320 else (int(320 * aspect_ratio), 320)
            resized_width, resized_height = ensure_even_dimensions(resized_width, resized_height)

            output_io = io.BytesIO()
            output = av.open(output_io, 'w', format='mp4')
            ostream = output.add_stream('libx264', rate=stream.average_rate)
            ostream.width = resized_width
            ostream.height = resized_height
            ostream.pix_fmt = 'yuv420p'

            for packet in video.demux(stream):
                for frame in packet.decode():
                    frame = frame.reformat(width=resized_width, height=resized_height)
                    for opacket in ostream.encode(frame):
                        output.mux(opacket)

            ostream.close()
            output.close()
            video_data = output_io.getvalue()

            sink.write({"__key__": os.path.basename(fpath), "mp4": video_data})
            video.close()

def ensure_even_dimensions(width, height):
    """确保尺寸为偶数"""
    if width % 2 != 0:
        width -= 1
    if height % 2 != 0:
        height -= 1
    return width, height

def make_tar_parallel(source_folder, output_folder, target_size=100e6, num_workers=1):
    assert os.path.isdir(source_folder), "Source folder not found"
    if not os.path.isdir(output_folder):
        os.makedirs(output_folder)

    # files = glob.glob(os.path.join(source_folder, "*.mp4"))
    files = glob.glob(os.path.join(source_folder, "**/*.mp4"), recursive=True)
    files = [file for file in files]
    files.sort()  # 可以按需要修改排序方式

    # 将文件列表分成几部分，为每个工作线程提供一个子列表
    num_files = len(files)
    chunk_size = (num_files + num_workers - 1) // num_workers  # 确保每个分区至少有一个文件

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = []
        for i in range(num_workers):
            # 计算每个工作线程的文件子集
            start_index = i * chunk_size
            end_index = min(start_index + chunk_size, num_files)
            file_subset = files[start_index:end_index]
            # 将任务提交到线程池
            futures.append(executor.submit(package_videos, file_subset, output_folder, i, target_size))
        
        # 等待所有工作线程完成
        for future in futures:
            future.result()  # 这将重新抛出工作线程中的任何异常

# 调用并行打包函数
# source_folder = "/home/guangyi.liu/shared-folder/datasets/guangyi_data/kinetics-dataset/k600/all"
# output_folder = "/home/guangyi.liu/shared-folder/datasets/guangyi_data/streaming/k600_wds_resize"
# source_folder = "/home/guangyi.liu/shared-folder/datasets/guangyi_data/activityNet/Anet_videos_15fps_short256"
# output_folder = "/home/guangyi.liu/shared-folder/datasets/guangyi_data/streaming/activitynet_wds_resize"
source_folder = "/home/guangyi.liu/shared-folder/datasets/guangyi_data/kinetics-dataset/k600/test"
output_folder = "/home/guangyi.liu/shared-folder/datasets/guangyi_data/streaming/k600_wds_resize_test"
make_tar_parallel(source_folder, output_folder, target_size=100e6, num_workers=64)
