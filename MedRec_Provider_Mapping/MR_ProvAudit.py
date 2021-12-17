from email import encoders
from smtplib import SMTPException
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
import smtplib
import pandas as pd 
import datetime
import time
import sys
import os
import glob

#add path to teradata class library
sys.path.append(r'E:\Installs\Python\Scripts\Classes\Teradata')
import Teradata_Class

#parameters-----------------------------------------------------------------------------------------------------
log_file_dir=r'E:\Installs\Python\Scripts\Projects\MedRec_Provider_Mapping\logs'
log_file_name_only='MR_ProvAudit_log'
data_exists_output = r'E:\Installs\Python\Scripts\Projects\MedRec_Provider_Mapping\Process_Files\Data1_Exists.txt'
email_sent_output = r'E:\Installs\Python\Scripts\Projects\MedRec_Provider_Mapping\Process_Files\Email1_Sent.txt'

smtp_server = 'smtp-gw.nas.medcity.net'
sender = 'DoNotReply@hcahealthcare.com'

#send_to = [
#'Tracie.Gault@Parallon.com'
#,'CaraLu.Royse@hcahealthcare.com'
#,'Jamison.Watkins@hcahealthcare.com'
#,'Charles.Argo@hcahealthcare.com'
#,'Natasha.Collins@hcahealthcare.com'
#,'BSC1.PSGDMBISupport@HCAHealthcare.com'
#]
send_to = ['Natasha.Collins@hcahealthcare.com']

copy_to = [
#'Natasha.Collins@hcahealthcare.com'
#'BSC1.PSGDMBISupport@HCAHealthcare.com'
]


audit_detail_sql = r'E:\Installs\Python\Scripts\Projects\MedRec_Provider_Mapping\SQL_Scripts\Medrec_Provider_Unmapped_COID.sql'
#-----------------------------------------------------------------------------------------------------------------

log_file_name_with_time = log_file_name_only + '_' + time.strftime("%Y%m%d_%H%M%S") + '.txt'
log_file = os.path.join(log_file_dir, log_file_name_with_time)

#function write to log file
def write_log(log_file, log_message):    
    with open(log_file, 'a') as f:    
        f.write(
            '['+datetime.datetime.now().strftime("%a %m/%d/%Y %#H:%M:%S.%f")[:-4]+'] {log_message}'
            .format(log_message=log_message)+'\n'
            )
    return


log_entry=write_log(log_file=log_file
                    ,log_message='MedRec provider unmapped provider audit process started.')
                    
#teradata actions
td_db = Teradata_Class.TD_DB()


#get audit detail sql
log_entry=write_log(log_file=log_file
                    ,log_message='Populate dataframe with Audit Detail teradata query {audit_detail_sql} started.'.format(
                        audit_detail_sql = audit_detail_sql))

with open(audit_detail_sql, 'r') as sql_file:
    sql = sql_file.read()

#populate dataframe with audit detail result
audit_detail_df = td_db.select_data_into_df(sql)

log_entry=write_log(log_file=log_file
                    ,log_message='Populate dataframe with Audit Detail teradata query {audit_detail_sql} ended successfully.'.format(
                        audit_detail_sql = audit_detail_sql))
#close teradata connection
td_db.close_td_connection()

if len(audit_detail_df.index) > 0:
    with open(data_exists_output, 'w') as f:					
		    f.write('1')

with open(data_exists_output, 'r') as f:
	fileExists = f.read()	
	
if fileExists == '0':
	log_entry=write_log(log_file=log_file
						,log_message='There are no unmapped providers, there should be no data in email.')	
	#plain text
	msg_text = """
	There are no unmapped providers for MedReceivables at this time.  
	"""	
	msg_text_html = """
	There are no unmapped providers for MedReceivables at this time. Please direct any questions to BSC1.PSGDMBISupport@HCAHealthcare.com.<br><br>Thanks!   
	"""		
	
	#smtp object
	log_entry=write_log(log_file=log_file
						,log_message='Send email message started.')

	smtpObj = smtplib.SMTP(smtp_server)
	msg = MIMEMultipart('alternative')

	msg['From'] = sender
	msg['To'] = ', '.join(send_to)
	msg['CC'] = ', '.join(copy_to)
	msg['Subject'] = 'MedRec Unmapped Providers Audit'

	TEXTpart = MIMEText(msg_text, 'plain')
	HTMLpart = MIMEText(msg_text_html, 'html') 

	msg.attach(msg_text)
	msg.attach(msg_text_html)

	try:
		smtpObj.send_message(msg)
		log_entry=write_log(log_file=log_file
						,log_message='Send email message ended successfully.')
		with open(email_sent_output, 'w') as f:					
			f.write('1')
		log_entry=write_log(log_file=log_file
						,log_message='Send email message ended successfully.')
		with open(email_sent_output, 'w') as f:					
			f.write('1')						
	except SMTPException:
		log_entry=write_log(log_file=log_file
						,log_message='Send email message error occurred, process unsuccessful.')
		log_entry=write_log(log_file=log_file
					,log_message='Failure: MedRec unmapped provider audit email process ended unsuccessfully.')
		with open(email_sent_output, 'w') as f:					
			f.write('0')

	log_entry=write_log(log_file=log_file
					,log_message='Success: MedRec unmapped provider audit email process ended successfully, but there were no unmapped providers')        	
	sys.exit()
else:
    log_entry=write_log(log_file=log_file
                        ,log_message='There are unmapped providers, there should be data in email.')
                        
    today = datetime.date.today()
    today_string = today.strftime("%B %d, %Y")

    #email message text
    log_entry=write_log(log_file=log_file
                        ,log_message='Build email message started.')


    #plain text
    msg_text = """
    The attached file contains the MedRec Unmapped Provider Audit results for data loaded through """ + today_string


    #html text
    msg_text_html = """
    <html>
    <body>
    <p>The attached file contains the MedRec Unmapped Provider Audit results for data loaded through """ + today_string + """.
    <br><br>
    Please direct any questions to BSC1.PSGDMBISupport@HCAHealthcare.com . <br><br> 
    Thanks!
    </body></html>
    """


    #prepare message
    ad_df = pd.DataFrame(audit_detail_df)
    text = msg_text.format(audit_detail_grid = ad_df.to_string(justify = 'center', index = 'false', na_rep = '?'), today_string = today_string)
    html = msg_text_html.format(audit_detail_grid = ad_df.to_html(justify = 'center', index = 'false', na_rep = '?'), today_string = today_string)
    export_wb = r'E:\Installs\Python\Scripts\Projects\MedRec_Provider_Mapping\Unmapped_Providers.xlsx'
                         
    with pd.ExcelWriter(export_wb) as writer:
        excelfile = ad_df.to_excel(writer)

    log_entry=write_log(log_file=log_file
                        ,log_message='Build email message ended successfully.')


    #smtp object
    log_entry=write_log(log_file=log_file
                        ,log_message='Send email message started.')

    smtpObj = smtplib.SMTP(smtp_server)
    msg = MIMEMultipart('alternative')

    msg['From'] = sender
    msg['To'] = ', '.join(send_to)
    msg['CC'] = ', '.join(copy_to)
    msg['Subject'] = 'MedRec Unmapped Providers Audit'


    # Create and attach MIMEText objects
    TEXTpart = MIMEText(msg_text, 'plain')
    HTMLpart = MIMEText(msg_text_html, 'html')
    
    msg.attach(TEXTpart)
    msg.attach(HTMLpart)


    # # prepare excel attachment                           
    fp = open(export_wb, 'rb')
    part = MIMEBase('application', 'vnd.ms-excel')
    part.set_payload(fp.read())
    fp.close()
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', 'attachment', filename='Unmapped_Providers.xlsx')
    msg.attach(part)


    # Send email
    try:
        with smtplib.SMTP(smtp_server) as server:
            server.sendmail(
                sender, send_to, msg.as_string()
            )
            server.quit()
        log_entry=write_log(log_file=log_file
                        ,log_message='Send email message ended successfully.')
        with open(email_sent_output, 'w') as f:
            f.write('1')

    except SMTPException:
        log_entry=write_log(log_file=log_file
                        ,log_message='Send email message error occurred, process unsuccessful.')
        log_entry=write_log(log_file=log_file
                    ,log_message='Failure: MedRec unmapped provider audit email process ended unsuccessfully.')
        with open(email_sent_output, 'w') as f:
            f.write('0')

                
#log file cleanup
log_entry=write_log(log_file=log_file
                ,log_message='Log file cleanup started.')


#delete all but 10 most recent job log files
files_10_most_recent = sorted([f for f in 
                               glob.glob(log_file_dir + os.path.sep + log_file_name_only + '*')
                               ], reverse = True)[:10]
for f in glob.glob(log_file_dir + os.path.sep + log_file_name_only + '*'):
    if f not in files_10_most_recent:
        os.remove(f)
        
log_entry=write_log(log_file=log_file
                ,log_message='Log file cleanup ended successfully.')
        
log_entry=write_log(log_file=log_file
                ,log_message='Success: MedRec unmapped provider email process ended successfully.')
sys.exit()        
