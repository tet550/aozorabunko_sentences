from typing import List
import glob
import datasets
import pandas as pd
import pyarrow.parquet as pq

class AozorabunkoSentences(datasets.ArrowBasedBuilder):
    VERSION = datasets.Version("1.0.0")

    def _info(self):
        return datasets.DatasetInfo(
            description="This is the dataset of Japanese Aozorabunko sentences.",
            features=datasets.Features({
                'title': datasets.Value('string'),
                'text': datasets.Value('string'),
            }),
            supervised_keys=None,
        )

    def _split_generators(self, dl_manager: datasets.DownloadManager):
        paths = glob.glob('data/*.parquet')
        return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'paths': paths})]

    def _generate_tables(self, paths: List[str]):
        idx = 0
        for path in paths:
            pa_table = pq.read_table(path)
            yield idx, pa_table
            idx += 1
    
    def _generate_examples(self, paths: List[str]):
        for path in paths:
            df = pd.read_parquet(path)
            for idx, row in df.iterrows():
                yield idx, {
                    'title': row['title'],
                    'text': row['text'],
                }

if __name__ == '__main__':
    # データセットを構築
    builder = AozorabunkoSentences()

    # キャッシュを無効化
    output_dir = 'cache/aozorabunko/'
    builder.download_and_prepare(output_dir=output_dir)
    dataset = builder.as_dataset()

    # 最初の数行を印刷
    for i in range(10):
        print(dataset['train'][i])

    dataset.push_to_hub("tet550/aozorabunko_sentences")