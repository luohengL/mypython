from pyecharts.charts import Pie
from pyecharts.charts import Timeline
import random
from pyecharts import options as opts
from pyecharts.faker import Faker

p1 = (
    Pie()
        .add("", [list(z) for z in zip(Faker.choose(), Faker.values())])
        .set_global_opts(title_opts=opts.TitleOpts(title="Pie-基本示例"))
        .set_series_opts(label_opts=opts.LabelOpts(formatter="{b}: {c}"))
)
p2 = (
    Pie()
        .add("", [list(z) for z in zip(Faker.choose(), Faker.values())])
        .set_global_opts(title_opts=opts.TitleOpts(title="Pie-基本示例"))
        .set_series_opts(label_opts=opts.LabelOpts(formatter="{b}: {c}"))
)
p3 = (
    Pie()
        .add("", [list(z) for z in zip(Faker.choose(), Faker.values())])
        .set_global_opts(title_opts=opts.TitleOpts(title="Pie-基本示例"))
        .set_series_opts(label_opts=opts.LabelOpts(formatter="{b}: {c}"))
)
p4 = (
    Pie()
        .add("", [list(z) for z in zip(Faker.choose(), Faker.values())])
        .set_global_opts(title_opts=opts.TitleOpts(title="Pie-基本示例"))
        .set_series_opts(label_opts=opts.LabelOpts(formatter="{b}: {c}"))
)

timeline = (Timeline().add_schema(is_auto_play=True, play_interval=1000)
            .add(p1, '第一季度')
            .add(p2, '第二季度')
            .add(p3, '第三季度')
            .add(p4, '第四季度')
            .render("全年销售图.html")
            )
