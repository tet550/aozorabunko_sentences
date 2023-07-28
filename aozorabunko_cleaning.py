import os
from pathlib import Path
import zipfile
import mojimoji
import re
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def process_zip_files(src_dir):
    """
    zipファイルを読み込み、テキストを抽出する
    :param src_dir: zipファイルのディレクトリ
    :return: テキスト
    """
    src_path = Path(src_dir)
    
    if not src_path.is_dir():
        raise Exception(f"Source directory '{src_dir}' does not exist.")
    
    for file in src_path.glob("**/*.zip"):
        try:
            with zipfile.ZipFile(file, 'r') as zip_ref:
                for zip_file in zip_ref.namelist():
                    if zip_file.endswith(".txt"):
                        with zip_ref.open(zip_file, 'r') as txt_file:
                            content = txt_file.read().decode("shift_jis", errors="ignore")
                            try:
                                title, year, text = process_content(content)
                            except Exception as e:
                                print(f"exception: {file} {zip_file} -> {e}")
                                
                            yield (title, year, text)
        except zipfile.BadZipFile:                            
            print(f"bad zip file: {file}")

def convert_fullwidth_to_halfwidth(text):
    """
    全角文字を半角文字に変換する
    :param text: 記事
    :return: 全角文字を半角文字に変換した記事
    """
    return mojimoji.zen_to_han(text, kana=False, ascii=True, digit=True)

def remove_parentheses_and_special_chars(text):
    """
    括弧と特殊文字を除去する
    :param text: 記事
    :return: 括弧と特殊文字を除去した記事
    """
    text = re.sub(r'[\t]+', ' ', text)             # タブ文字の削除
    text = re.sub(r'[\u3000\xa0]+', ' ', text)       # 全角・半角スペースの削除
    text = re.sub(r'^\s*\w+\s*$\n?', '', text, flags=re.MULTILINE) # ゴミ制御文削除
    text = re.sub(r'^[ ]*$\n', '', text, flags=re.MULTILINE) # 空行の削除
    return text

def process_content(text):
    """
    テキストを整形する
    :param text: テキスト
    :return: タイトル、日付、本文
    """
    text = text.replace('\r\n', '\n')  # CRLF (Windows) を LF に変換
    text = text.replace('\r', '\n')    # CR (Mac) を LF に変換
    title, text = text.split("\n", 1)  # 最初の改行で分割
    text = re.sub(r"^(.|\n)*?\n\s*\n", "", text)
    text = re.sub(r'--------*(.|\n)*?--------*', "", text)

    split_text = re.split('\n.*底本：', text, 1)
    if len(split_text) == 1:
        split_text = text.rsplit('\n\n\n', 1)
    if len(split_text) == 1:
        split_text = text.rsplit('\n\n', 1)
    if len(split_text) == 1:
        # 例外をスローする
        raise Exception(f"Invalid format: {text}")

    if len(split_text) == 2:
        # 分割したテキストの2番目（"底本：" 以降）から日付を検索する
        regex = r'(\d{4}).*?年(\d{1,2})月(\d{1,2})日'
        match = re.search(regex, split_text[1])
        if not match:
            match = re.search(r'(\d{4}).*?年', split_text[1])        
        if match:
            year = match.group(1)
            month = match.group(2)
            day = match.group(3)
            datetime = f'{year}/{month}/{day}'
        text = split_text[0]

    # year が2030以上であれば例外をスローする
    if int(year) >= 2030:
        raise Exception(f"Invalid year: {split_text[1]}")
    
    text = re.sub(r'｜(.+?)《.*?》', r'\1', text)
    text = re.sub(r'《.*?》', '', text)
    text = re.sub(r'［＃.*］', '', text)
    text = re.sub(r'#', '＃', text)
    text = re.sub(r'^ *\n', '', text, flags=re.MULTILINE)
    text = convert_fullwidth_to_halfwidth(text)
    text = remove_parentheses_and_special_chars(text)
    return (title, year, text)


def write_output(file_path, output_dir):
    """
    ダンプファイルを読み込み、Parquetファイルに書き出す
    :param file_path: ダンプファイルのパス
    :param output_dir: Parquetファイルの出力先ディレクトリ
    """    
    os.makedirs(output_dir, exist_ok=True)

    schema = pa.schema([('title', pa.string()), 
                        ('text', pa.string())])

    article_counter = 1
    for title, year, text in process_zip_files(file_path):

        if article_counter % 100 == 0:
            print(f'{article_counter} articles processed.')
        article_counter += 1

        # text を改行で分割する
        text_list = text.split('\n')
        # 末尾が空行なら削除する
        if text_list[-1] == '':
            text_list.pop()
        # text_list を title とのペアのリストにする
        article_list = [(title, text) for text in text_list]
        
        # year の末尾の数字を x にする
        year = year[:-1] + 'x'
        writer = pq.ParquetWriter(f'{output_dir}aozorabunko_{year}.parquet', schema)
       
        dataframe = pd.DataFrame(article_list, columns=['title', 'text'])
        batch = pa.RecordBatch.from_pandas(dataframe, schema=schema)
        writer.write_table(pa.Table.from_batches([batch]))
        
        writer.close()



if __name__ == '__main__':

    file_path = "../aozorabunko/cards/"
    output_path = "data/"
    
    write_output(file_path, output_path)
