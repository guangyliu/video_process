from scenedetect import detect, ContentDetector,open_video,StatsManager,SceneManager
import numpy as np
def detect_cut(mp4file):
    video = open_video(mp4file)
    video.seek(1) #  the first frame may give large delta_edge value, start from the second frame
    stats_manager = StatsManager()
    scene_manager = SceneManager(stats_manager)
    scene_manager.add_detector(ContentDetector())
    scene_manager.detect_scenes(video=video)
    frame_keys = stats_manager._frame_metrics.keys()  # read metrics
    delta_edges = []
    delta_lums = []
    for key in frame_keys:
        delta_edges.append(stats_manager._frame_metrics[key]['delta_edges'])
        delta_lums.append(stats_manager._frame_metrics[key]['delta_lum'])
    de_mean = np.mean(delta_edges)
    de_std = np.std(delta_edges)
    dl_mean = np.mean(delta_lums)
    dl_std = np.std(delta_lums)
    de_upper_bound = max(de_mean + 5.5 * de_std, 3* de_mean)
    dl_upper_bound = max(dl_mean + 5.5 * dl_std, 3* dl_mean)
    total_cut_de = np.sum(np.array(delta_edges) > de_upper_bound )
    total_cut_dl = np.sum(np.array(delta_lums) > dl_upper_bound )
    return total_cut_de> 0 or total_cut_dl >0 # if True, there is a cut, ignore this sample