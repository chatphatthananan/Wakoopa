import SGTAMProdTaskConfig as config

import sqlalchemy as sql
import logging
import sys

class SGTAMProd:

	def __init_db_connection(self, database):
		"""To initialise database connection"""

		server = 'xxx'
		username = config.db_username
		password = config.db_password
		self.engine = sql.create_engine(f"mssql+pymssql://{username}:{password}@{server}/{database}")


	def execute_query_to_df(self, sql_query, database):
		"""To execute query and output to dataframe

		Parameter:
		sql_query : str
			SQL query to be executed.
			example :
				SELECT * FROM tLog

		Example:
		from SGTAMProdTask import SGTAMProd
		s = SGTAMProd()
		sql_query = 'SELECT TOP 10 * FROM tLog ORDER BY logDtTime DESC'
		df = s.execute_query_to_df(sql_query=sql_query)
		print(df)
		"""

		import pandas as pd
		self.__init_db_connection(database=database)
		try:
			with self.engine.connect() as con:
				df = pd.read_sql(sql=sql_query, con=con)
				return df
		except Exception as e:
			logging.exception(f'Error executing query: {e}, {sql_query}')
			sys.exit(f'Error executing query: {e}, {sql_query}')


	def execute_query_with_result(self, sql_query, database):
		"""To execute query and output to list

		Parameter:
		sql_query : str
			SQL query to be executed.
			example :
				SELECT * FROM tLog

		Return:
		list
			SQL query result in list

		Example:
		from SGTAMProdTask import SGTAMProd
		s = SGTAMProd()
		sql_query = 'SELECT TOP 10 * FROM tLog ORDER BY logDtTime DESC'
		result = s.execute_query_with_result(sql_query=sql_query)
		print(result[0][0])
		"""

		self.__init_db_connection(database=database)
		try:
			with self.engine.begin() as con:
				rs = con.execute(sql_query)
				return rs.fetchall()
		except Exception as e:
			logging.exception(f'Error executing query: {e}, {sql_query}')
			sys.exit(f'Error executing query: {e}, {sql_query}')


	def execute_query_without_result(self, sql_query, database):
		"""To execute query and output to list

		Parameter:
		sql_query : str
			SQL query to be executed.
			example :
				SELECT * FROM tLog

		Example:
		from SGTAMProdTask import SGTAMProd
		s = SGTAMProd()
		sql_query = 'UPDATE tLog SET logMsg = 'testing' 
					 WHERE logID = '7FD70F84-2BC2-4721-ABB5-F1BF87549D12''
		s.execute_query_without_result(sql_query=sql_query)
		"""

		self.__init_db_connection(database=database)
		try:
			with self.engine.begin() as con:
				con.execute(sql_query)				
		except Exception as e:
			logging.exception(f'Error executing query: {e}, {sql_query}')
			sys.exit(f'Error executing query: {e}, {sql_query}')


	def __validate_tlog_kwargs(self, **kwargs):
		"""To validate tLog parameters to ensure required keys are there
		
		Parameter:
		kwargs : dict
			expecting dictionary including keys of logTaskID, statusFlag, logMsg and logID
			example :
				SGTAM_log_config = {'logTaskID' : -99,
									'statusFlag' : 2,
									'logMsg' : 'task has started',
									'logID' : None}
		"""

		if not 'logTaskID' in kwargs:
			logging.error('logTaskID not found!')
			sys.exit('logTaskID not found!')

		if not 'statusFlag' in kwargs:
			logging.error('statusFlag not found!')
			sys.exit('statusFlag not found!')

		if not 'logMsg' in kwargs:
			logging.error('logMsg not found!')
			sys.exit('logMsg not found!')

		if not 'logID' in kwargs:
			logging.error('logID not found!')
			sys.exit('logID not found!')


	def insert_tlog(self, **kwargs):
		"""To insert tLog for SGTAMProdTaskLog, it will return 2 variables, statusFlag and logID to be used for update later.

		Parameter:
		kwargs : dict
			expecting dictionary including keys of logTaskID, statusFlag, logMsg and logID
			example :
				SGTAM_log_config = {'logTaskID' : -99,
									'statusFlag' : 2,
									'logMsg' : 'task has started',
									'logID' : None}
		Return:
		int
			SGTAMProd tLog status flag 
		UUID
			SGTAMProd logID

		Example:
		from SGTAMProdTask import SGTAMProd
		s = SGTAMProd()
		SGTAM_log_config = {'logTaskID' : -99,
							'statusFlag' : 2,
							'logMsg' : 'task has started',
							'logID' : None}
		SGTAM_log_config['statusFlag'], SGTAM_log_config['logID']  = s.insert_tlog(**SGTAM_log_config)
		"""

		self.__validate_tlog_kwargs(**kwargs)
		logging.info("Insert into tLog and retrieve logID")
		sql_query = f"EXEC SP_LogAdd {kwargs['logTaskID']}, {kwargs['statusFlag']}, '{kwargs['logMsg']}'"
		ds = self.execute_query_with_result(sql_query=sql_query, database='SGTAMProd')
		logging.info(f"Created logID: {ds[0].logID}")
		return 1, ds[0].logID


	def __validate_update_tlog_kwargs(self, **kwargs):
		"""To validate tLog parameters to ensure required keys are there before update tLog
		
		Parameter:
		kwargs : dict
			expecting dictionary including keys of logTaskID, statusFlag, logMsg and logID
			example :
				SGTAM_log_config = {'logTaskID' : -99,
									'statusFlag' : 2,
									'logMsg' : 'task has started',
									'logID' : None}
		"""

		if kwargs['logID'] == None:
			logging.exception('logID is blank!')
			sys.exit('logID is blank!')


	def update_tlog(self, **kwargs):
		"""To update tLog for SGTAMProdTaskLog

		Parameter:
		kwargs : dict
			expecting dictionary including keys of logTaskID, statusFlag, logMsg and logID
			example :
				SGTAM_log_config = {'logTaskID' : -99,
									'statusFlag' : 2,
									'logMsg' : 'task has started',
									'logID' : None}

		Example:
		from SGTAMProdTask import SGTAMProd
		s = SGTAMProd()
		SGTAM_log_config = {'logTaskID' : -99,
							'statusFlag' : 2,
							'logMsg' : 'task has started',
							'logID' : None}
		SGTAM_log_config['statusFlag'], SGTAM_log_config['logID']  = s.insert_tlog(**SGTAM_log_config)

		SGTAM_log_config['logMsg'] = "this is 't '' test 123"
		s.update_tlog(**SGTAM_log_config)
		"""

		self.__validate_tlog_kwargs(**kwargs)
		self.__validate_update_tlog_kwargs(**kwargs)
		kwargs['logMsg'] = kwargs['logMsg'].replace("'", "''")

		logging.info(f"Updating tLog logID: {kwargs['logID']} with status: {kwargs['statusFlag']}")
		sql_query = f"EXEC SP_LogUpd '{kwargs['logID']}', '{kwargs['statusFlag']}', '{kwargs['logMsg']}'"
		self.execute_query_without_result(sql_query=sql_query, database='SGTAMProd')


	def is_holiday(self, ref_date, include_weekend):
		"""To check if ref_date is holiday

		Parameter:
		ref_date : str
			reference date you wish to check if is holiday
			example :
				'2022-04-28'

		include_weekend : int
			only 1 and 0 are accepted, 1 = Yes, 0 = No
			if factor in weekend as holiday then pass 1, else 0
			example :
				1

		Return:
		boolean
			With given reference date and include weekend parameter, it returns True (Is Holiday) or False (Is Not Holiday).

		Example:
		from SGTAMProdTask import SGTAMProd
		s = SGTAMProd()
		if s.is_holiday(ref_date='2022-04-28', include_weekend=1):
			print('execute task')

		if s.is_holiday(ref_date='2022-04-30', include_weekend=1):
			print('execute task')

		if s.is_holiday(ref_date='2022-04-30', include_weekend=0):
			print('execute task')

		if s.is_holiday(ref_date='2022-05-01', include_weekend=1):
			print('execute task')

		if s.is_holiday(ref_date='2022-05-01', include_weekend=0):
			print('execute task')
		"""

		valid_include_weekend_code = [1, 0]
		if include_weekend not in valid_include_weekend_code:
			logging.exception(f"Invalid include_weekend parameter: {include_weekend}, expecting values: {valid_include_weekend_code}")
			sys.exit(f"Invalid include_weekend parameter: {include_weekend}, expecting values: {valid_include_weekend_code}")
		
		sql_query = f"SELECT dbo.fnGetSkipExecutionResultBasedOnHoliday('{ref_date}', {include_weekend}) AS SkipExecution"

		result = self.execute_query_with_result(sql_query=sql_query, database='EvoProd')

		if result[0][0] == 1:
			logging.info(f'{ref_date} is holiday. Include weekend: {include_weekend}')
			return True
		elif result[0][0] == 0:
			logging.info(f'{ref_date} is not holiday. Include weekend: {include_weekend}')
			return False


	def __validate_pre_requisite_log_kwargs(self, **kwargs):
		"""To validate pre-requisite log parameters to ensure required keys are there
		
		Parameter:
		kwargs : dict
			expecting dictionary including keys of logTaskID and allowedStatus
			example :
				pre_requisite_log = {
				'Prelim PLD V3 SFTP Upload' : {'logTaskID' : 88, 'allowedStatus' : [1,3]},
				'Check Prelim PLD V3 SFTP' : {'logTaskID' : 89, 'allowedStatus' : [1]},
			}
		"""

		ref_allowed_status_code = set([-1, 1, 2, 3])

		for k, v in kwargs.items():
			if not 'logTaskID' in v:
				logging.error(f'logTaskID not found in {k}!')
				sys.exit(f'logTaskID not found in {k}!')
			
			if not 'allowedStatus' in v:
				logging.error(f'allowedStatus not found in {k}!')
				sys.exit(f'allowedStatus not found in {k}!')

			raw_allowed_status_code = set(v['allowedStatus'])
			invalid_allowed_status_code = raw_allowed_status_code - ref_allowed_status_code
			if len(invalid_allowed_status_code) > 0:
				logging.error(f'Invalid allowedStatus: {invalid_allowed_status_code} in {k}! Expected status: {ref_allowed_status_code}')
				sys.exit(f'Invalid allowedStatus: {invalid_allowed_status_code} in {k}! Expected status: {ref_allowed_status_code}')


	def is_SGTAMProd_log_task_passed(self, ref_date, **kwargs):
		"""To check if pre-requisite SGTAMProd log task passed

		Parameter:
		ref_date : str
			reference date you wish to check for particular SGTAMProd Log Task
			example : 
				'2022-04-28'

		kwargs : dict
			expecting dictionary including keys of pre-requisite task name, a sub-dictionary of logTaskID and allowedStatus			
			logTaskID : int
				should be valid logTaskID from tLogTask that is found in tLog as at ref_date
			allowedStatus : list
				allowed status of logStatus, expecting range from -1, 1, 2 and 3 only.
				these are statuses where it allowed that particular logTaskID to proceed as pass, return True as a result for subsequent action. 
			example :
				pre_requisite_log = {
					'Prelim PLD V3 SFTP Upload' : {'logTaskID' : 88, 'allowedStatus' : [1,3]},
					'Check Prelim PLD V3 SFTP' : {'logTaskID' : 89, 'allowedStatus' : [1]},
				}

		Return:
		boolean
			With given reference date and pre-requisite dicts parameter, it returns True (Passed) or False (Not Passed).

		Example:
		from SGTAMProdTask import SGTAMProd
		s = SGTAMProd()
		pre_requisite_log = {
			'Prelim PLD V3 SFTP Upload' : {'logTaskID' : 88, 'allowedStatus' : [1,3]},
			'Check Prelim PLD V3 SFTP' : {'logTaskID' : 89, 'allowedStatus' : [1]},
		}

		if s.is_SGTAMProd_log_task_passed('2022-04-29', **pre_requisite_log):
    		print('execute task')
		"""

		self.__validate_pre_requisite_log_kwargs(**kwargs)		
		is_passed = True

		for k, v in kwargs.items():
			logging.info(f"Get logTaskStatus {k}: {v['logTaskID']} on {ref_date}")
			sql_query = f"EXEC SP_GetLatestLogStatusByLogTaskID {v['logTaskID']}, '{ref_date}'"
			result = self.execute_query_with_result(sql_query=sql_query, database='SGTAMProd')

			log_status = -1 if len(result) == 0 else int(result[0][2])

			if log_status in v['allowedStatus']:
				logging.info(f"LogTask '{k}' : {v['logTaskID']} logStatus {log_status} matched with allowed status: {v['allowedStatus']}!")				
			else:
				logging.warning(f"LogTask '{k}' : {v['logTaskID']} logStatus {log_status} does not match with allowed status: {v['allowedStatus']}!")
				is_passed = False
		
		if is_passed:
			logging.info("SGTAMProd Log passed!")
			return True
		else:
			logging.info("SGTAMProd Log not passed!")
			return False


	def __validate_email_kwargs(self, **kwargs):
		"""To validate email parameters to ensure required keys are there
		
		Parameter:
		kwargs : dict
			expecting dictionary including keys of logTaskID and allowedStatus
			example :
				email = {
					'to' : 'edwin.yap@gfk.com',
					'bcc' : 'edwin.yap@gfk.com',
					'subject' : 'test 1234',
					'body' : body,
					'is_html' : True,
					'filename' : 'attachment.txt'
				}
		"""

		if not 'subject' in kwargs:
			logging.error('subject not found!')
			sys.exit('subject not found!')

		if not 'body' in kwargs:
			logging.error('body not found!')
			sys.exit('body not found!')

		if not any(key in kwargs for key in ['to', 'cc', 'bcc']):
			logging.error('to/cc/bcc not found!')
			sys.exit('to/cc/bcc not found!')

		if not isinstance(kwargs['is_html'], bool):
			logging.error('is_html must be boolean!')
			sys.exit('is_html must be boolean!')


	def send_email(self, **kwargs):
		"""To send email
		
		Parameter:
		kwargs : dict
			expecting dictionary including keys of to/cc/bcc, subject, body, is_html, attachment, sender
			to/cc/bcc : str
				receipt to receive email
			subject : str
				email subject
			body : str
				email body
			is_html : bool
				email body is html or not
			attachment : str
				file directory to be attached in the email
			sender : str
				default value is SGTAMProd@gfk.com
				you can override the value if you need to
			example :
				email = {
					'to' : 'edwin.yap@gfk.com',
					'bcc' : 'edwin.yap@gfk.com',
					'subject' : 'test 1234',
					'body' : 'body testing 1234',
					'is_html' : True,
					'filename' : 'attachment.txt'
				}

		Example:
		from SGTAMProdTask import SGTAMProd
		s = SGTAMProd()
		body = "<h1>Hi, this is HTML body</h1>"
		email = {
			'sender' : 'edwin.yap@gfk.com',
			'to' : 'edwin.yap@gfk.com',
			'subject' : 'test 1234',
			'body' : body,
			'is_html' : False,
			'filename' : 'attachment.txt'
		}
		s.send_email(**email)
		"""

		import smtplib
		from email import encoders
		from email.mime.multipart import MIMEMultipart
		from email.mime.base import MIMEBase
		from email.mime.text import MIMEText
		import os
		
		self.__validate_email_kwargs(**kwargs)

		email = MIMEMultipart('alternative')
		
		email['Subject'] = kwargs['subject']
		email['From'] = 'xxx' if not 'sender' in kwargs else kwargs['sender']
		email['To'] = None if not 'to' in kwargs else kwargs['to']
		email['CC'] = None if not 'cc' in kwargs else kwargs['cc']
		email['BCC'] = None if not 'bcc' in kwargs else kwargs['bcc']

		if kwargs['is_html']:
			email_text = MIMEText(kwargs['body'], 'html')
		else:
			email_text = MIMEText(kwargs['body'], 'plain')

		email.attach(email_text)
		if 'filename' in kwargs:
			if len(kwargs['filename']) > 0:
				with open(kwargs['filename'], "rb") as attachment:
					attch = MIMEBase("application", "octet-stream")
					attch.set_payload(attachment.read())
				encoders.encode_base64(attch)

				attch.add_header(
					"Content-Disposition",
					"attachment", filename=os.path.basename(kwargs['filename'])
				)
				email.attach(attch)

		with smtplib.SMTP('mailout.gfk.com', 25) as s:
			s.send_message(email)
