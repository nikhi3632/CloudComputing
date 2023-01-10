import boto3
from boto3.dynamodb.conditions import Key
import urllib.parse
import face_recognition
import pickle
import os, re, sys, csv


s3 = boto3.client('s3')
S3 = boto3.resource('s3')
input_bucket = "cc546-bandits-input"
output_bucket = "cc546-bandits-output"
dynamodb = boto3.resource('dynamodb')
dyname_table_name = 'student_data'

# Function to read the 'encoding' file
def open_encoding(filename):
	file = open(filename, "rb")
	data = pickle.load(file)
	file.close()
	return data

def get_items_from_dynamo(db, table_name):
	table = db.Table(table_name)
	response = table.scan()
	return response['Items'] 

def get_data_from_dynamo(db_data, column_key, column_value):
	column_values = [data[column_key] for data in db_data]
	if not column_value in column_values:
		print('Not found!')
		sys.exit(1)
	else:
		for d in db_data:
			if d[column_key] == column_value:
				return d				

def write_to_csv_and_upload_to_s3(csv_name, fields, field_names, bucket_name):
	required_dict = {}
	for key in field_names:
		required_dict[key] = fields[key]
	path = "/tmp/"
	filename = csv_name + '.csv'
	csv_file = path + filename
	with open(csv_file, 'w') as csvfile:
		writer = csv.DictWriter(csvfile, fieldnames = field_names)
		writer.writeheader()
		writer.writerows([required_dict])
	location = 's3://{}/'.format(bucket_name)
	upload_file_to_s3(filename, bucket_name, path, location)
	if os.path.exists(csv_file):
		os.remove(csv_file)
		print("Deleted " + csv_file)

def upload_file_to_s3(file, bucket_name, upload_path, location):
    
    upload_file_path = upload_path + '/' + file
    try:
        s3.upload_file(
            upload_file_path,
            bucket_name,
            Key = file
        )
    except Exception as e:
        print("Exception occurred: ", e)
        return e
    return print("File uploaded to S3 bucket {}: {}{}".format(bucket_name, location, file))

def face_recognition_handler(event, context):	
	print("Hello")
	
	#Downloading the video from the S3 input bucket
	bucket = event['Records'][0]['s3']['bucket']['name']
	print(bucket)
	key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
	print(key)
	try:
		path = "/tmp/"
		S3.Bucket(bucket).download_file(key, path + key)
		os.system("ffmpeg -i " + str(path + key) + " -r 1 " + str(path) + key[:-4] + "_image-%3d.jpeg")
		dir_list = os.listdir(path)
		print(dir_list)

		#Deleting Video
		if os.path.exists(path+ key):
			os.remove(path+ key)
			print("Deleted " + key)
		pattern = "^" + key[:-4]
		
		
		#Image recognition
		count = len(os.listdir(path))
		data = open_encoding('encoding')
		known_faces = data['encoding']
		frames = os.listdir(path)
		frames.sort()
		for i in frames:
			unknown_image = face_recognition.load_image_file(path + i)
			try:
				unknown_face_encoding = face_recognition.face_encodings(unknown_image)[0]
				break
			except IndexError:
				print("I wasn't able to locate any faces in at least one of the images. Check the image files. Aborting...")
				sys.exit(1)
		results = face_recognition.compare_faces(known_faces, unknown_face_encoding)

		#Deleting the frames
		for f in os.listdir(path):
			if re.search(pattern,f):
				os.remove(os.path.join(path,f))
		
		index = results.index(True)
		name = data['name'][index]
		print(name)

		#Getting information from DB and storing in s3 bucket
		dynamo_db_data = get_items_from_dynamo(dynamodb,dyname_table_name)
		details = get_data_from_dynamo(dynamo_db_data, 'name', name)
		print(details)
		csv_name = key[:-4]
		write_to_csv_and_upload_to_s3(csv_name, details, ['name', 'major', 'year'], output_bucket)
		dir_list = os.listdir(path)
		print(dir_list)
		
		return "Completed"
	except Exception as e:
		print(e)
		print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
		raise e
	
	