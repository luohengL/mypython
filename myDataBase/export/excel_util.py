from datetime import datetime


class ExcelUtil:
    export_path = 'excel/'

    t = datetime.now()

    def generate_path_by_prefix(self, name_prefix='export'):
        return self.export_path + name_prefix + (
                u'_%d%02d%02d_%02d%02d%02d.xlsx' % (t.year, t.month, t.day, t.hour, t.minute, t.second))

    def generate_path_by_name(self,name='export'):
        return self.export_path + name;



