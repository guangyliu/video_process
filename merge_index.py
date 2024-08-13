import json
import os
import fire

def main(root):
   infos = []
   for subdir in sorted(os.listdir(root)):
      filename = os.path.join(root, subdir, 'index.json')
      obj = json.load(open(filename))
      for shard in range(len(obj['shards'])):
         for which in ['raw_data', 'zip_data', 'raw_meta', 'zip_meta']:
            if obj['shards'][shard].get(which):
                  basename = obj['shards'][shard][which]['basename']
                  obj['shards'][shard][which]['basename'] = os.path.join(subdir, basename)
      infos += obj['shards']
   obj = {
      'version': 2,
      'shards': infos,
   }
   filename = os.path.join(root, 'index.json')
   with open(filename, 'w') as out:
      json.dump(obj, out)


if __name__ == "__main__":
    fire.Fire(main)