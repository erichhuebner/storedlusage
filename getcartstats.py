#!/usr/bin/python

import sys
import time
import csv
import MySQLdb
import smtplib
from datetime import date, timedelta, datetime
from dateutil.relativedelta import *
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email.MIMEText import MIMEText
from email import Encoders
import os
import stat
import socket


def mail(to, subject, text, cc=None, bcc=None, reply_to=None, attach=None,
         html=None, pre=False, custom_headers=None):

    #user = "email@example.com"
    #passwd = "password"
    from_user = "Hubname Reports <reports@hubname.org>"

    msg = MIMEMultipart()

    msg['From'] = from_user
    msg['To'] = to
    msg['Subject'] = subject

    #to = [to]

    if type(to) in [str, unicode]:
        #msg.add_header('Cc', cc)
        to = [to]

    if cc:

        if type(cc) in [str, unicode]:
            msg.add_header('Cc', cc)
            cc = [cc]
        else:
            for item in cc:    
                #newcc = ', '.join(cc)
                msg.add_header('Cc', item)
        to += cc

        '''
        # cc gets added to the text header as well as list of recipients
        if type(cc) in [str, unicode]:
            msg.add_header('Cc', cc)
            cc = [cc]
        else:
            cc = ', '.join(cc)
            msg.add_header('Cc', cc)
        to += cc
        '''

    if bcc:
        # bcc does not get added to the headers, but is a recipient
        if type(bcc) in [str, unicode]:
            bcc = [bcc]

        to += bcc

    if reply_to:
        msg.add_header('Reply-To', reply_to)

    # Encapsulate the plain and HTML versions of the message body in an
    # 'alternative' part, so message agents can decide which they want to
    # display.

    if pre:
        html = "<pre>%s</pre>" % text
    if html:
        msgAlternative = MIMEMultipart('alternative')
        msg.attach(msgAlternative)

        msgText = MIMEText(text)
        msgAlternative.attach(msgText)

        # We reference the image in the IMG SRC attribute by the ID we give it
        # below
        msgText = MIMEText(html, 'html')
        msgAlternative.attach(msgText)
    else:
        msg.attach(MIMEText(text))

    if attach:
        if type(attach) in [str, unicode]:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(open(attach, 'rb').read())
            Encoders.encode_base64(part)
            part.add_header('Content-Disposition',
                            'attachment; filename="%s"' % os.path.basename(attach))
            msg.attach(part)

        else:
            for item in attach:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(open(item, 'rb').read())
                Encoders.encode_base64(part)
                part.add_header('Content-Disposition',
                                'attachment; filename="%s"' % os.path.basename(item))
                msg.attach(part)

    if custom_headers:
        for k, v in custom_headers.iteritems():
            msg.add_header(k, v)

    mailServer = smtplib.SMTP('localhost')
    #mailServer = smtplib.SMTP("smtp.gmail.com", 587)
    #mailServer.ehlo()
    #mailServer.starttls()
    #mailServer.ehlo()
    #mailServer.login(gmail_user, gmail_pwd)

    #mailServer.sendmail(gmail_user, to, msg.as_string())
    mailServer.sendmail(from_user, to, msg.as_string())
    # Should be mailServer.quit(), but that crashes...
    mailServer.close()


def getdailyrecords(cursor, row, startdate, date_db_start, date_db_end):

    csvrow = []

    sql_day = "SELECT dDownloaded,pId,COUNT(*) as count FROM(SELECT uId,p.pId,pName,dDownloaded FROM `jos_cart_downloads` d LEFT JOIN `jos_storefront_skus` s ON (s.sId = d.sId) LEFT JOIN `jos_storefront_products` p ON (s.pId = p.pId) WHERE dDownloaded >= '" + date_db_start + "' AND dDownloaded <= '" + date_db_end + "' AND sSKU IS NOT NULL AND p.pId =" + str(row[0]) + " GROUP BY uId,s.sId) AS dusers;"

    cursor.execute(sql_day)
    product_count = cursor.fetchall()

    sql_all = "SELECT dDownloaded,pId,COUNT(*) as count FROM(SELECT uId,p.pId,pName,dDownloaded FROM `jos_cart_downloads` d LEFT JOIN `jos_storefront_skus` s ON (s.sId = d.sId) LEFT JOIN `jos_storefront_products` p ON (s.pId = p.pId) WHERE dDownloaded <= '" + date_db_end + "' AND sSKU IS NOT NULL AND p.pId =" + str(row[0]) + " GROUP BY uId,s.sId) AS dusers;"

    cursor.execute(sql_all)
    product_count_culmulative = cursor.fetchall()

    csvrow = (str(row[1]), str(product_count[0][2]), str(product_count_culmulative[0][2]))

    return csvrow

def getweeklyrecords(cursor, row, startdate, date_db_start, date_db_end):

    csvrow = []

    sql_week = "SELECT dDownloaded,pId,COUNT(*) as count FROM(SELECT uId,p.pId,pName,dDownloaded FROM `jos_cart_downloads` d LEFT JOIN `jos_storefront_skus` s ON (s.sId = d.sId) LEFT JOIN `jos_storefront_products` p ON (s.pId = p.pId) WHERE dDownloaded >= '" + date_db_start + "' AND dDownloaded <= '" + date_db_end + "' AND sSKU IS NOT NULL AND p.pId =" + str(row[0]) + " GROUP BY uId,s.sId) AS dusers;"

    cursor.execute(sql_week)
    product_count = cursor.fetchall()


    sql_all = "SELECT dDownloaded,pId,COUNT(*) as count FROM(SELECT uId,p.pId,pName,dDownloaded FROM `jos_cart_downloads` d LEFT JOIN `jos_storefront_skus` s ON (s.sId = d.sId) LEFT JOIN `jos_storefront_products` p ON (s.pId = p.pId) WHERE dDownloaded <= '" + date_db_end + "' AND sSKU IS NOT NULL AND p.pId =" + str(row[0]) + " GROUP BY uId,s.sId) AS dusers;"

    cursor.execute(sql_all)
    product_count_culmulative = cursor.fetchall()

    csvrow = (str(row[1]), str(product_count[0][2]), str(product_count_culmulative[0][2]))

    return csvrow

def getmonthlyrecords(cursor, row, startdate, date_db_start, date_db_end):

    csvrow = []


    sql_month = "SELECT dDownloaded,pId,COUNT(*) as count FROM(SELECT uId,p.pId,pName,dDownloaded FROM `jos_cart_downloads` d LEFT JOIN `jos_storefront_skus` s ON (s.sId = d.sId) LEFT JOIN `jos_storefront_products` p ON (s.pId = p.pId)WHERE dDownloaded >= '" + date_db_start + "' AND dDownloaded <= '" + date_db_end + "'  AND sSKU IS NOT NULL AND p.pId =" + str(row[0]) + " GROUP BY uId,s.sId) AS dusers;"

    cursor.execute(sql_month)
    product_count = cursor.fetchall()

    sql_all = "SELECT dDownloaded,pId,COUNT(*) as count FROM(SELECT uId,p.pId,pName,dDownloaded FROM `jos_cart_downloads` d LEFT JOIN `jos_storefront_skus` s ON (s.sId = d.sId) LEFT JOIN `jos_storefront_products` p ON (s.pId = p.pId) WHERE dDownloaded <= '" + date_db_end + "' AND sSKU IS NOT NULL AND p.pId =" + str(row[0]) + " GROUP BY uId,s.sId) AS dusers;"

    cursor.execute(sql_all)
    product_count_culmulative = cursor.fetchall()

    csvrow = (str(row[1]), str(product_count[0][2]), str(product_count_culmulative[0][2]))

    return csvrow

def processdailyrecords(filepath, cursor, products, dateformats):

    #dateformats = (0-startdate,1-yesterday_db_start,2-yesterday_db_end,3-yesterday_file,4-vorgestern_file,5-yesterday_csv,6-week_db_start,7-month_db_start,8-week_file,9-month_file,10-lastweek_csv_start,11-lastmonth_csv_start)

    if os.path.isfile('%sdaily/daily_storefront_downloadusers_%s.csv' % (filepath, dateformats[4])):
        os.system ("cp -p %sdaily/daily_storefront_downloadusers_%s.csv %sdaily/daily_storefront_downloadusers_%s.csv" % (filepath, dateformats[4], filepath, dateformats[3]))
        time.sleep(3)

    product_total = 0
    cumulative_total = 0

    with open('%sdaily/daily_storefront_downloadusers_%s.csv' % (filepath,dateformats[3]),'ab') as out:
        csv_out=csv.writer(out)
        csv_out.writerow(['',''])
        csv_out.writerow(['','' ])
        csv_out.writerow(['Product', dateformats[5], 'Cumulative' ])
        for row in products:
            csvrow = getdailyrecords(cursor, row, dateformats[0], dateformats[1], dateformats[2])
            csv_out.writerow(csvrow)
            product_total = product_total + int(csvrow[1])
            cumulative_total = cumulative_total + int(csvrow[2])

        csv_out.writerow(['','' ])
        csv_out.writerow(['Totals', product_total, cumulative_total])


def processweeklyrecords(filepath, cursor, products, dateformats):

    #dateformats = (0-startdate,1-yesterday_db_start,2-yesterday_db_end,3-yesterday_file,4-vorgestern_file,5-yesterday_csv,6-week_db_start,7-month_db_start,8-week_file,9-month_file,10-lastweek_csv_start,11-lastmonth_csv_start)

    if os.path.isfile('%sweekly/weekly_storefront_downloadusers_%s.csv' % (filepath,dateformats[8])):
        os.system ("cp -p %sweekly/weekly_storefront_downloadusers_%s.csv %sweekly/weekly_storefront_downloadusers_%s.csv" % (filepath,dateformats[8], filepath,dateformats[3]))
        time.sleep(3)

    product_total = 0
    cumulative_total = 0

    with open('%sweekly/weekly_storefront_downloadusers_%s.csv' % (filepath,dateformats[3]),'ab') as out:
        csv_out=csv.writer(out)
        csv_out.writerow(['',''])
        csv_out.writerow(['','' ])
        csv_out.writerow(['Product', dateformats[10] + ' - ' + dateformats[5], 'Cumulative' ])
        for row in products:
            csvrow = getweeklyrecords(cursor, row, dateformats[0], dateformats[6], dateformats[2])
            csv_out.writerow(csvrow)
            product_total = product_total + int(csvrow[1])
            cumulative_total = cumulative_total + int(csvrow[2])

        csv_out.writerow(['','' ])
        csv_out.writerow(['Totals', product_total, cumulative_total])

def processmonthlyrecords(filepath, cursor, products, dateformats):

    #dateformats = (0-startdate,1-yesterday_db_start,2-yesterday_db_end,3-yesterday_file,4-vorgestern_file,5-yesterday_csv,6-week_db_start,7-month_db_start,8-week_file,9-month_file,10-lastweek_csv_start,11-lastmonth_csv_start)

    if os.path.isfile('%smonthly/monthly_storefront_downloadusers_%s.csv' % (filepath,dateformats[9])):
        os.system ("cp -p %smonthly/monthly_storefront_downloadusers_%s.csv %smonthly/monthly_storefront_downloadusers_%s.csv" % (filepath,dateformats[9], filepath,dateformats[3]))
        time.sleep(3)

    product_total = 0
    cumulative_total = 0

    with open('%smonthly/monthly_storefront_downloadusers_%s.csv' % (filepath,dateformats[3]),'ab') as out:
        csv_out=csv.writer(out)
        csv_out.writerow(['',''])
        csv_out.writerow(['','' ])
        csv_out.writerow(['Product', dateformats[11] + ' - ' + dateformats[5], 'Cumulative' ])
        for row in products:
            csvrow = getmonthlyrecords(cursor, row, dateformats[0], dateformats[7], dateformats[2])
            csv_out.writerow(csvrow)
            product_total = product_total + int(csvrow[1])
            cumulative_total = cumulative_total + int(csvrow[2])

        csv_out.writerow(['','' ])
        csv_out.writerow(['Totals', product_total, cumulative_total])     


def main():

    hostname = socket.gethostname() 
    if hostname == "otherhost.hubname.org":
        exit()

    daily_on = 0  #1 = on, 0 = off, One must be on
    weekly_on = 1
    monthly_on = 1
    email_on = 1
    
    startdate = '2016-08-08 00:00:00'
    filepath = '/some/path/'

    if len(sys.argv) == 2:
        today =  datetime.strptime(str(sys.argv[1]),'%Y-%m-%d')
    else:
        today =  date.today()
        #today =  datetime.strptime('2017-11-01','%Y-%m-%d')
 
        #today =  datetime.strptime('2017-11-01','%Y-%m-%d')
        

    yesterday = today - timedelta(days=1) #2016-08-20
    vorgestern = today - timedelta(days=2)
    lastweek_end = (today - relativedelta(weeks=1)) - timedelta(days=1)
    lastmonth_end = (today - relativedelta(months=1)) - timedelta(days=1)
    lastweek_start = today - relativedelta(weeks=1)
    lastmonth_start = today - relativedelta(months=1)
    
    
    yesterday_db_start = yesterday.strftime("%Y-%m-%d 00:00:00")
    yesterday_db_end = yesterday.strftime("%Y-%m-%d 23:59:59")
    week_db_start = lastweek_start.strftime("%Y-%m-%d 00:00:00")
    month_db_start = lastmonth_start.strftime("%Y-%m-%d 00:00:00")
    
    yesterday_file = yesterday.strftime("%Y%m%d")
    vorgestern_file = vorgestern.strftime("%Y%m%d")
    week_file = lastweek_end.strftime("%Y%m%d")
    month_file = lastmonth_end.strftime("%Y%m%d")
    
    yesterday_csv = yesterday.strftime("%m/%d/%Y")
    lastweek_csv_start = lastweek_start.strftime("%m/%d/%Y")
    lastmonth_csv_start = lastmonth_start.strftime("%m/%d/%Y")

    dateformats = (startdate,yesterday_db_start,yesterday_db_end,yesterday_file,vorgestern_file,yesterday_csv,week_db_start,month_db_start,week_file,month_file,lastweek_csv_start,lastmonth_csv_start)
    #dateformats = (0-startdate,1-yesterday_db_start,2-yesterday_db_end,3-yesterday_file,4-vorgestern_file,5-yesterday_csv,6-week_db_start,7-month_db_start,8-week_file,9-month_file,10-lastweek_csv_start,11-lastmonth_csv_start)

    #Connect to the database
    db = MySQLdb.connect(host="127.0.0.1",user="someusename",passwd="somepassword",db="somedbname")
    # prepare a cursor object using cursor() method
    cursor = db.cursor()

    #Get active products
    #cursor.execute("SELECT pId, pName FROM `jos_storefront_products` WHERE pActive = '1';")
    cursor.execute("SELECT pId, pName FROM `jos_storefront_products` WHERE pName NOT LIKE '%Test%' AND pName NOT LIKE '%Happy%' ORDER by pName ASC;")
    products = cursor.fetchall()

    attachment = []

    #Setup and send mail  **expects lists for multiple recipients

    to = 'someone@purdue.edu'
    cc=['someone@purdue.edu','someone@purdue.edu']
    bcc='someone@purdue.edu'
    reply_to='someone@purdue.edu'
    subject = 'Storefront Downloads Report'
    text = "Please see the attached file for today's usage report."
    
    if daily_on == 1:
        processdailyrecords(filepath, cursor, products, dateformats)
        attachment.append('%sdaily/daily_storefront_downloadusers_%s.csv' % (filepath, dateformats[3]))
        if email_on == 1:
            mail('someone@purdue.edu', subject, text, reply_to=reply_to, attach=attachment)

    if weekly_on == 1 and today.weekday() == 0:
        #if day is a Monday, Monday is 0 and Sunday is 6
        processweeklyrecords(filepath, cursor, products, dateformats)
        attachment.append('%sweekly/weekly_storefront_downloadusers_%s.csv' % (filepath, dateformats[3]))
        if email_on == 1:
            mail(to, subject, text, cc=cc, bcc=bcc, reply_to=reply_to, attach=attachment)

    if monthly_on == 1 and today.strftime("%d") == '01':
        #if day is the fist of the month
        processmonthlyrecords(filepath, cursor, products, dateformats)
        attachment.append('%smonthly/monthly_storefront_downloadusers_%s.csv' % (filepath, dateformats[3]))
        if email_on == 1:
            mail(to, subject, text, cc=cc, bcc=bcc, reply_to=reply_to, attach=attachment)

    db.close()
if __name__ == "__main__":
    main()
