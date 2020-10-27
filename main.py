import download_csv as cdv
import upload_file as cuf

tablename = 'corona_virus_brasil'
file_type = '.csv'
file = tablename + file_type
save_path = '/home/ec2-user/scaranni/arquivos/download'
file_folder = '/home/ec2-user/scaranni/arquivos/'
file_full_path = f'/home/ec2-user/scaranni/arquivos/{file}'

cdv.extract(save_path, file_folder)
cuf.load(file_full_path, tablename)


