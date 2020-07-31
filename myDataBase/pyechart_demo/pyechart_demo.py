from pyecharts.charts import Bar
from pyecharts import options as opts
from pyecharts.faker import Faker



bar = Bar()
bar.add_xaxis(["衬衫", "羊毛衫", "雪纺衫", "裤子", "高跟鞋", "袜子"])
bar.add_yaxis("商家A", [5, 20, 36, 10, 75, 90], stack="stack1")
bar.add_yaxis("商家B", [15, 23, 16, 14, 35, 120],stack="stack1")
# render 会生成本地 HTML 文件，默认会在当前目录生成 render.html 文件
# 也可以传入路径参数，如 bar.render("mycharts.html")
# bar.reversal_axis()
bar.set_series_opts(label_opts=opts.LabelOpts(is_show=False,position="right"))
bar.set_global_opts(title_opts=opts.TitleOpts(title="Bar-堆叠数据（部分）"),datazoom_opts=opts.DataZoomOpts())
bar.render("pyechart_demo.html")


