__author__ = 'vasu'
from datetime import datetime
import sys, os, subprocess, tarfile, smtplib, re, shutil
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

curr_year = datetime.now().strftime('%Y')
curr_month = datetime.now().strftime('%B')
month_start_date = datetime.now().strftime('%d')
curr_date = datetime.now().strftime('%d-%m-%Y')
curr_day = datetime.now().strftime('%A')
failed_db_list = ''
log_file_location = '/srv/appcontent/databasebackups/dump_log.txt'
daily = 7
weekly = 4
yearly = 6

# path='"/"+curr_year+"/"+curr_month+"/"+curr_date'
def print_usage(script):
    print('Usage:', script, '--cnf <config file>', '--todir <directory>')
    sys.exit(1)


def usage(args):
    if not len(args) == 5:
        print_usage(args[0])
    else:
        req_args = ['--cnf', '--todir']
        for a in req_args:
            if not a in req_args:
                print_usage()
            if not os.path.exists(args[args.index(a) + 1]):
                print('Error: Path not found:', args[args.index(a) + 1])
                print_usage()
    cnf = args[args.index('--cnf') + 1]
    if month_start_date == "01":
        yearly_path = args[args.index('--todir') + 1] + "/"+"monthly"
        create_root_directories(yearly_path)
        rotate_backups(yearly_path, yearly)
        path = yearly_path + "/" + curr_date
    elif curr_day == 'Sunday':
        weekly_path = args[args.index('--todir') + 1] + "/" +"weekly"
        create_root_directories(weekly_path)
        rotate_backups(weekly_path, weekly)
        path = weekly_path + "/" + curr_date
    else:
        daily_path=args[args.index('--todir') + 1] + "/" + "daily"
        create_root_directories(daily_path)
        rotate_backups(daily_path, daily)
        path = daily_path + "/" + curr_date
    try:
        os.makedirs(path)
    except OSError:
        if not os.path.isdir(path):
            raise
    dir = path
    return cnf, dir

def create_root_directories(root_path):
    try:
        os.makedirs(root_path)
    except OSError:
        if not os.path.isdir(root_path):
            raise


def rotate_backups(dir_path, rotate_count):
    string_to_date_list=[]
    date_format_list=[]
    root, dirs, files = next(os.walk(dir_path))
    for dir in dirs:
        string_to_date_list.append(datetime.strptime(dir, '%d-%m-%Y').date())
    string_to_date_list.sort()
    date_format_list = [datetime.strftime(stod, "%d-%m-%Y") for stod in string_to_date_list]
    dir_count = len(date_format_list)
    if dir_count >= rotate_count:
        shutil.rmtree(os.path.join(root,date_format_list[0]))



def mysql_dblist(cnf):
    no_backup = ['Database', 'information_schema', 'performance_schema', 'test']
    cmd = ['mysql', '--defaults-extra-file=' + cnf, '-e', 'show databases']
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    if p.returncode > 0:
        write_logs('cannot cannect to database backup process failed'+ "\n")
        sys.exit(1)
    dblist = stdout.decode('utf8').strip().split('\n')
    for item in no_backup:
        try:
            dblist.remove(item)
        except ValueError:
            continue
    if len(dblist) == 1:
        print("Doesn't appear to be any user databases found")
    return dblist


def mysql_backup(dblist, dir, cnf):
    for db in dblist:
        bdate = datetime.now().strftime('%d-%m-%Y-%H-%M')
        bfile = bdate+'_'+db + '.sql'
        dumpfile = open(os.path.join(dir, bfile), 'w')
        if db == 'mysql':
            cmd = ['mysqldump', '--defaults-extra-file=' + cnf, '--events', db]
        else:
            cmd = ['mysqldump', '--defaults-extra-file=' + cnf, '--single-transaction' ,'--triggers', '--routines', '--events',  '--databases', '--add-drop-database', '--default-character-set=utf8', db]
        p = subprocess.Popen(cmd, stdout=dumpfile)
        retcode = p.wait()
        dumpfile.close()
        if retcode > 0:
            global failed_db_list
            failed_db_list = 'Failed DB Dump List'
            failed_db_list += db+', '
        backup_compress(dir, bfile)

#def check_version():


def backup_compress(dir, bfile):
    
    tar = tarfile.open(os.path.join(dir, bfile) + '.tar.gz', 'w:gz')
    tar.add(os.path.join(dir, bfile), arcname=bfile)
    tar.close()
    os.remove(os.path.join(dir, bfile))


def log_status():
    if failed_db_list:
        write_logs(failed_db_list)
    else:
        write_logs("success")

def write_logs(log_data):
    with open(log_file_location, "a") as logfile:
        logfile.write('------------'+curr_date+'-----------------'+"\n")
        logfile.write(log_data+"\n")
    

def main():
    cnf, dir = usage(sys.argv)
    dblist = mysql_dblist(cnf)
    mysql_backup(dblist, dir, cnf)
    log_status()


if __name__ == '__main__':
    main()
