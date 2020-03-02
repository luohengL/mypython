import jieba
from wordcloud import WordCloud


# 读取弹幕文件
def read_file(file_name):
    with open(file_name,mode='r',encoding='utf-8') as f:
        dan_mu = f.read()
        return dan_mu


# jieba fenci
def jieba_cut(str):
    jieba.suggest_freq('家里全是狗',tune=True)
    cut_list = jieba.lcut(str)
    return cut_list


# 生成词云图
def generate_wrod_cloud(cut_list):
    word_str = ' '.join(cut_list)
    wc_settings = {
        'font_path': 'C:\Windows\Fonts\simfang.ttf',
        'width': 800,
        'height': 600,
        'background_color': 'white',
        'max_words': 100
    }
    wc = WordCloud(**wc_settings).generate(word_str)

    wc.to_file('华农.png')


if __name__ == '__main__':
    av = 'av88149130'
    str = read_file(f'{av}.txt')
    cut_list = jieba_cut(str)
    generate_wrod_cloud(cut_list)
