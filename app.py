import os
import codecs

import flask
from flask import Flask, render_template,request
import mysql.connector
import csv
import datetime



app = flask.Flask(__name__)
app.config["DEBUG"] = True
dir_path = os.path.dirname(os.path.realpath(__file__))
app.secret_key = b'_5#y2L"F4Q8z\n\xec]/'

 # Obtain connection string information from the portal
# config = {
#   'host':'azureserver.mysql.database.azure.com',
#   'user':'azure_admin',
#   'password':'CloudComputing@123',
#   'database':'retailapp',
#   'tls_versions': ['TLSv1.1', 'TLSv1.2'],
#   'ssl_ca':'DigiCertGlobalRootCA.crt.pem'
# }
config = {
  'host':'tejuservercloud.mysql.database.azure.com',
  'user':'tejuserver@tejuservercloud',
  'password':'Kt@220399',
  'database':'ccsqldb',
  'port': 3306,
#   'ssl_ca': os.path.join(dir_path, 'BaltimoreCyberTrustRoot.crt.pem'),
#   'ssl_verify_cert': 'true',
  'connect_timeout':50
}

# Construct connection string
try:
   conn = mysql.connector.connect(**config)
   print("Connection established")
except mysql.connector.Error as err:
  if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
    print("Something is wrong with the user name or password")
  elif err.errno == errorcode.ER_BAD_DB_ERROR:
    print("Database does not exist")
  else:
    print(err)
else:
  cursor = conn.cursor()


@app.route('/')
def login():
    return render_template("login.html")

@app.route('/dashboard')
def tableaudashboard():
    return render_template("tableau.html")

@app.route('/register')
def register():
    return render_template("register.html")

@app.route('/loginValidation',methods=['POST'])
def login_validation():
    error=None
    #conn=mysql.connector.connect(user='azureadmin', password='Admin@123',host='azurecloudassign.database.windows.net',database='azurecloud')
   # conn = pyodbc.connect(
   #     'DRIVER={SQL Server};SERVER=tcp:azurecloudassign.database.windows.net,1433;DATABASE=azurecloud;UID=azureadmin;PWD=Admin@123;')
    #cursor = conn.cursor()
    username=request.form['Uname']
    password=request.form['Pass']
    cursor.execute("""SELECT * FROM users where username = %s and password = %s """,(username, password,))
    usersss=cursor.fetchone()
    conn.commit()
    if usersss != None :
        sql_select_query = "select h.hshd_num, t.basket_num,t.product_num,t.purchase_,p.department,p.commodity,t.spend,t.units,t.store_r,t.week_num,t.year,h.L,h.age_range,h.marital,h.income_range,h.homeowner,h.hshd_composition,h.hh_size,h.children from households as h,transactions as t , products as p where h.hshd_num=t.hshd_num and t.product_num=p.product_num and h.hshd_num=10 order by h.hshd_num,t.BASKET_NUM,t.purchase_,t.PRODUCT_NUM,p.DEPARTMENT,p.COMMODITY"
        cursor.execute(sql_select_query)
        data = cursor.fetchall()
        #conn.close()
        return render_template("homepage.html", hshddata=data)
    else:
        error="Incorrect username or password"
        return render_template('login.html',error=error)

@app.route('/addUser',methods=['POST'])
def add_user():
    msg="Registered Sucessfully. Please login to continue"
    #conn=mysql.connector.connect(user='azureadmin', password='Admin@123',
    #                          host='tcp:azurecloudassign.database.windows.net,1433',
     #                         database='azurecloud')
    #conn = pyodbc.connect(
    #   'DRIVER={SQL Server};SERVER=tcp:azurecloudassign.database.windows.net,1433;DATABASE=azurecloud;UID=azureadmin;PWD=Admin@123;')
    c = conn.cursor()
    fnameu=request.form['Fname']
    lnameu=request.form['Lname']
    emailu=request.form['email']
    usernameu=request.form['username']
    pswu=request.form['Pass']
    c.execute("""SELECT * FROM users WHERE username= %s""",(usernameu,))
    users=c.fetchone()
    if users==None:
        c.execute("""INSERT INTO users (username, email, password) VALUES (%s,%s,%s)""",(usernameu,emailu,pswu,))
        conn.commit()
        #conn.close()
        msg="Registration Successful!"
        return render_template("login.html",msg=msg)
    else:
        error="Username already exists. Please try with different Username"
        #conn.close()
        return render_template("register.html",error=error)


@app.route('/getrows', methods=['GET','POST'])
def getRows():
    hhnum=request.args.get('search')
   # hhnum=request.form['search']
    #print(hhnum)
    #conn=mysql.connector.connect(user='azureadmin', password='Admin@123',
    #                          host='tcp:azurecloudassign.database.windows.net,1433',
     #                         database='azurecloud')
    #conn = pyodbc.connect(
     #   'DRIVER={SQL Server};SERVER=tcp:azurecloudassign.database.windows.net,1433;DATABASE=azurecloud;UID=azureadmin;PWD=Admin@123;')
    #cursor = conn.cursor()

    #sql_select_query = "select h.hshd_num, t.basket_num,t.product_num,t.purchase,p.department,p.commodity,t.spend,t.units,t.store_r,t.week_num,t.year,h.L,h.age_range,h.marital,h.income_range,h.homeowner,h.hshd_composition,h.hh_size,h.children from households as h,transactions as t , products as p where h.hshd_num=t.hshd_num and t.product_num=p.product_num and h.hshd_num=?"
    cursor.execute("""select h.hshd_num, t.basket_num,t.product_num,t.purchase_,p.department,p.commodity,t.spend,t.units,t.store_r,t.week_num,t.year,h.L,h.age_range,h.marital,h.income_range,h.homeowner,h.hshd_composition,h.hh_size,h.children from households as h,transactions as t , products as p where h.hshd_num=t.hshd_num and t.product_num=p.product_num and h.hshd_num=%s order by h.hshd_num,t.BASKET_NUM,t.purchase_,t.PRODUCT_NUM,p.DEPARTMENT,p.COMMODITY""",(hhnum,) )
    data=cursor.fetchall()
    #conn.close()
    return render_template("homepage.html",hshddata=data)

def decode_utf8(input_iterator):
    for l in input_iterator:
        yield l.decode('utf-8')

@app.route('/insertdata', methods=['GET', 'POST'])
def insertCSVData():
    tablename= None
    errmsg=None
    fileName = request.files['file']
    #conn=mysql.connector.connect(user='azureadmin', password='Admin@123',
     #                         host='tcp:azurecloudassign.database.windows.net,1433',
      #                        database='azurecloud')
    #conn = pyodbc.connect(
    #    'DRIVER={SQL Server};SERVER=tcp:azurecloudassign.database.windows.net,1433;DATABASE=azurecloud;UID=azureadmin;PWD=Admin@123;')
    #cursor = conn.cursor()

    if 'product' in fileName.filename.lower():
        reader = csv.reader(codecs.iterdecode(request.files['file'], 'utf-8'))
        next(reader)
        products_insert = 'INSERT INTO products(PRODUCT_NUM, DEPARTMENT, COMMODITY,brand_type,NATURAL_ORGANIC_FLAG) VALUES '
        if reader:
            tablename='PRODUCTS'
            for row in reader:
                products_insert += '{},'.format(tuple(row))
            products_insert = products_insert[:len(products_insert) - 1]
            cursor.execute(products_insert)

    elif 'household' in fileName.filename.lower():
        reader = csv.reader(codecs.iterdecode(request.files['file'], 'utf-8'))
        next(reader)
        households_insert = 'INSERT INTO households(HSHD_NUM,L,AGE_RANGE,MARITAL,INCOME_RANGE,HOMEOWNER,HSHD_COMPOSITION,HH_SIZE,CHILDREN) VALUES '
        if reader:
            tablename='HOUSEHOLDS'
            for row in reader:
                households_insert += '{},'.format(tuple(row))
            households_insert = households_insert[:len(households_insert) - 1]
            cursor.execute(households_insert)

    elif 'transaction' in fileName.filename.lower():
        tablename='TRANSACTIONS'
        sheet = fileName.read()
        transactions_insert = 'INSERT INTO transactions (BASKET_NUM, HSHD_NUM, purchase_, PRODUCT_NUM, SPEND,UNITS,store_r,WEEK_NUM,YEAR) VALUES '
        for line in csv.DictReader(sheet.decode().splitlines(), skipinitialspace=True):
            transactions_insert += '{},'.format((list(line.values())[0], list(line.values())[1], list(line.values())[2], list(line.values())[3],list(line.values())[4],list(line.values())[5],list(line.values())[6],list(line.values())[7],list(line.values())[8], ))
        transactions_insert = transactions_insert[:len(transactions_insert) - 1]
        cursor.execute(transactions_insert)

    else:
        errmsg="Incorrect file provided. Allowed filenames households.csv or transactions.csv or products.csv"
    conn.commit()
    sql_select_query = "select h.hshd_num, t.basket_num,t.product_num,t.purchase_,p.department,p.commodity,t.spend,t.units,t.store_r,t.week_num,t.year,h.L,h.age_range,h.marital,h.income_range,h.homeowner,h.hshd_composition,h.hh_size,h.children from households as h,transactions as t , products as p where h.hshd_num=t.hshd_num and t.product_num=p.product_num and h.hshd_num=10 order by h.hshd_num,t.BASKET_NUM,t.purchase_,t.PRODUCT_NUM,p.DEPARTMENT,p.COMMODITY"
    cursor.execute(sql_select_query)
    data = cursor.fetchall()
    insert_msg="Data is inserted successfully in the table"
    #conn.close()
    if errmsg==None:
        return render_template("homepage.html", hshddata=data,insertmsg=insert_msg,tablename=tablename)
    else:
        return render_template("homepage.html",hshddata=data,errmsg=errmsg)


if __name__=="__main__":
    app.run(port=8009)
