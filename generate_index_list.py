from streaming import StreamingDataset
from typing import Any
import os
from tqdm import tqdm,trange
import json
import shutil
import fire

class PandaDataset(StreamingDataset):
    def __init__(
        self,
        remote: str,
        **kwargs: Any
        ):
        super().__init__(remote=remote,
                         **kwargs)

    def __getitem__(self, idx: int):
        sample = super().__getitem__(idx)
        return sample, idx

def process_collate(batch):
    tar_name_list = [sample[0]['tar_name'].split('/panda70m/')[-1] for sample in batch]
    id_list = [sample[1] for sample in batch]

    return [tar_name_list, id_list]

def main(root=None):
    if not root:
        data_path='/home/guangyi.liu/shared-folder/datasets/panda70m_streaming_all_2s_10s_2xframe_ofm1.5_cutnum_aes4.5_forcaption_gpt_v4_cut_cond4'
    else:
        data_path = root
    post_name = data_path.split('_')[-1]
    print(post_name)
    # dataset = PandaDataset(remote=data_path,shuffle=False, num_canonical_nodes=1)
    from torch.utils.data import DataLoader
    # dataloader = DataLoader(dataset, 512, num_workers=64, collate_fn=process_collate)
    
    
    local_path = f'/scratch/generate_id12'
    
    dataset = PandaDataset(remote=data_path,shuffle=False, local=local_path)
    dataloader = DataLoader(dataset, 512, num_workers=64, collate_fn=process_collate)
    
    tar_name_all = []
    id_all = []
    for batch in tqdm(dataloader,total=len(dataloader), ncols=80):
        tar_name_list = batch[0]
        tar_name_all.extend(tar_name_list)
        id_list = batch[1]
        id_all.extend(id_list)
    # 创建一个临时的列表，其中每个元素都是(tar_name, id)的元组
    combined = list(zip(tar_name_all, id_all))
    
    # 按tar_name排序combined列表
    combined.sort(key=lambda x: x[0])
    
    # 解压排序后的列表回到tar_name_all和id_all
    tar_name_all_sorted, id_all_sorted = zip(*combined)
    
    # 将结果转换回列表形式
    tar_name_all_sorted = list(tar_name_all_sorted)
    id_all_sorted = list(id_all_sorted)
    tar_id_dict  = {tar_name:[] for tar_name in set(tar_name_all_sorted)}
    for i in range(len(tar_name_all_sorted)):
        cur_tar_name = tar_name_all_sorted[i]
        tar_id_dict[cur_tar_name].append(int(id_all_sorted[i]))
    
    filename = f'/home/guangyi.liu/shared-folder/datasets/guangyi_data/scripts/panda70m/streaming_tar_id/id_list_sorted_gpt_cut_{post_name}_k600.json'
    id_all_sorted_native = [int(item) for item in id_all_sorted]
    
    # 使用with语句和open函数打开文件，'w'表示写入模式
    with open(filename, 'w') as f:
        # 使用json.dump将数据写入文件
        json.dump(id_all_sorted_native, f)
    
    filename = f'/home/guangyi.liu/shared-folder/datasets/guangyi_data/scripts/panda70m/streaming_tar_id/tar_id_dict_gpt_cut_{post_name}_k600.json'
    
    # 使用with语句和open函数打开文件，'w'表示写入模式
    with open(filename, 'w') as f:
        # 使用json.dump将数据写入文件
        json.dump(tar_id_dict, f)
    if os.path.exists(local_path):
        shutil.rmtree(local_path)


if __name__ == "__main__":
    fire.Fire(main)