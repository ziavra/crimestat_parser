# -*- coding: utf-8 -*-

import re, urllib2, urllib, sys, json, httplib, sqlite3, logging, traceback, csv
from fixLazyJson import fixLazyJsonWithComments
from csvUnicode import UnicodeWriter

__author__ = 'ziavra'

DATABASE_FILE = 'social_portrait.sqlite'


class CrimestatParser(object):
    csDomain = 'crimestat.ru'
    baseUrl = 'http://'+csDomain+'/'
#   собрано руками со страницы http://crimestat.ru/social_portrait, лежит для информации
    __data_slice = {14002:	'по возрасту',
                    14005:	'по образованию',
                    14006:	'по социальному составу',
                    14007:	'по принадлежности к гражданству'}

    def __init__(self):
        # create logger
        self.logger = logging.getLogger("crimestat_parser")
        self.logger.setLevel(logging.DEBUG)

        # create console handler and set level to debug
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)

        # create formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # add formatter to ch
        ch.setFormatter(formatter)

        # add ch to logger
        self.logger.addHandler(ch)

        # 'application' code
        self.logger.debug('Parser started')
        self.con = sqlite3.connect(DATABASE_FILE, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)

    def __get_web_page(self, url, ref='', query={}):
        user_agent = 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.0'
        headers = {'User-Agent': user_agent, 'Referer': ref}

        data = urllib.urlencode(query, True)

        req = urllib2.Request(url, data if data else None, headers)
        html=''
        try:
            response = urllib2.urlopen(req)
        except urllib2.HTTPError, e:
            self.logger.error('HTTPError = ' + str(e.code))
        except urllib2.URLError, e:
            self.logger.error('URLError = ' + str(e.reason))
        except httplib.HTTPException, e:
            self.logger.error(e)
        except Exception:
            self.logger.error('generic exception: ' + traceback.format_exc())
            raise
        else:
            html = response.read()

        return html

    def __row_generator_params(self, data):
        for row in data:
            yield (row[0], row[1])

    def __row_generator_data(self, region, period, data_slice, cat, gender, data):
        for row in zip(cat, data):
            yield (None, region, period, data_slice, row[0], gender, row[1])

    def refresh_params(self):
        self.logger.debug('Refresh started')
        html = self.__get_web_page(self.baseUrl+'social_portrait', self.baseUrl)
        if not html:
            return False

#       DEBUG
        with open('tmp.html', 'w') as f:
            f.write(html)

#        html = unicode(html, 'unicode-escape')
        html = unicode(html, 'utf-8')

#       получаем регионы
        regions = re.findall('<option value="(\d+)">(.+?)</option>', html, re.U)
        if regions:
            try:
                with self.con:
                    self.con.executemany("INSERT OR REPLACE INTO regions VALUES(?, ?)", self.__row_generator_params(regions))
            except:
                self.logger.error("Unexpected error:" + repr(sys.exc_info()[0]))
                raise

#       получаем периоды
        periods = re.findall(u'<option value="(\d{2}\.\d{4})">за (\d{4}) г\.</option>', html, re.U)
        if periods:
            try:
                with self.con:
                    self.con.executemany("INSERT OR REPLACE INTO periods VALUES(?, ?)", self.__row_generator_params(periods))
            except:
                self.logger.error("Unexpected error:" + repr(sys.exc_info()[0]))
                raise

        return True

    def get_data(self, force_update=False):
        cur = self.con.cursor()
        if force_update:
            try:
                self.con.execute("delete from data ")
            except :
                self.logger.error("Unexpected error:" + repr(sys.exc_info()[0]))
                raise

        # декартово произведение регионов, дат и видов срезов, для которых нет данных в таблице
        sql = ( 'select r.id, p.id, ds.id '
                'from regions r, '
                'periods p, '
                'data_slices ds '
                'LEFT OUTER JOIN data d '
                'on d.data_slice=ds.id '
                'and d.period=p.id '
                'and d.region=r.id '
                'where d.id is NULL')

        for row in cur.execute(sql):
            region, period, data_slice = row
            url = self.baseUrl+'object/'+ urllib.quote_plus(str(data_slice))
            html = self.__get_web_page(url, self.baseUrl+'social_portrait', {'reg_code':region, 'dt':period})
            if not html:
                self.logger.warning("Can't get url " + url)
                continue

#           DEBUG
#            with open('tempdata/ds%s_r%s_p%s.html' % (data_slice, region, period), 'w') as f:
#                f.write(html)

            html = unicode(html, 'cp1251')
            data = re.search('Highcharts\.Chart\((\{.*\})\);', html, re.MULTILINE | re.DOTALL)
            if data:
                json_data=data.group(1)
#                with open('tempdata/ds%s_r%s_p%s.json' % (data_slice, region, period), 'w') as f:
#                    f.write(json_data.encode('utf-8'))
                self.logger.debug('loaded url %s?reg_code=%d&dt=%s' % (url, region, period))
            else:
                self.logger.debug('failed url %s?reg_code=%d&dt=%s' % (url, region, period))
                continue

#           dirty hack чтобы убрать JS-функции из JSON и обойтись стандартными библиотеками
            json_data = re.sub("function\(.*?;\s+?\}\s+}", "'x'}", json_data, 0, re.MULTILINE | re.DOTALL)
            # fix для имен полей без кавычек в JSON
            # см. http://stackoverflow.com/questions/4033633/handling-lazy-json-in-python-expecting-property-name
            json_data = fixLazyJsonWithComments(json_data)
#            with open('tempdata/ds%s_r%s_p%s_fixed.json' % (data_slice, region, period), 'w') as f:
#                f.write(json_data.encode('utf-8'))
            graph_data=json.loads (json_data)

            cat=graph_data[u'xAxis'][u'categories']
            if not cat:
                continue

            series=graph_data[u'series']
            for item in series:
                if item[u'name']==u'Мужчин':
                    m_data=item[u'data']
                elif item[u'name']==u'Женщин':
                    f_data=map(lambda x: abs(int(x)), item[u'data'])

            if not len(cat) == len(m_data) == len(f_data):
                self.logger.error("Bad data: categories size - %d, male data size - %d, female data size - %d" %(len(cat),len(m_data),len(f_data)))
                continue

            # Данные по мужчинам
            try:
                with self.con:
                    self.con.executemany("INSERT OR IGNORE INTO data VALUES(?, ?, ?, ?, ?, ?, ?)", self.__row_generator_data(region, period, data_slice, cat, 'm', m_data))
            except :
                self.logger.error("Unexpected error:" + repr(sys.exc_info()[0]))
                raise

            # Данные по женщинам
            try:
                with self.con:
                    self.con.executemany("INSERT OR IGNORE INTO data VALUES(?, ?, ?, ?, ?, ?, ?)", self.__row_generator_data(region, period, data_slice, cat, 'f', f_data))
            except :
                self.logger.error("Unexpected error:" + repr(sys.exc_info()[0]))
                raise

    def export_to_csv(self, filename, slices=()):
        self.logger.debug('Export started')
        cur = self.con.cursor()

        sql = ( 'select r.desc, p.desc, ds.desc, d.category, d.gender, d.data '
                'from regions r, '
                'periods p, '
                'data_slices ds '
                'LEFT OUTER JOIN data d '
                'on d.data_slice=ds.id '
                'and d.period=p.id '
                'and d.region=r.id '
                'where d.id is not NULL ')

        if slices:
            cond=', '.join(map(str, slices))
            sql+=' and d.data_slice in ('+cond+')'

        try:
            cur.execute(sql)
            rows = cur.fetchall()
        except :
            self.logger.error("Unexpected error:" + repr(sys.exc_info()[0]))
            raise

        if not rows:
            return False

        with open(filename, 'wb') as csvfile:
            csvwriter = UnicodeWriter(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            csvwriter.writerow([u"Регион", u"Период", u"Срез", u"Категория", u"Пол", u"Значение"])
            for row in rows:
                csvwriter.writerow(map(unicode, row))

################################################################################
# Program entrypoint.
if __name__ == "__main__":
    csp = CrimestatParser()
    csp.refresh_params()
    csp.get_data()
#    csp.export_to_csv('test.csv', (14002, 14005)) #выгрузить только определенные срезы
    csp.export_to_csv('crimestat_social_portrait.csv')
