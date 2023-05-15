import Data_loader as dl
import pandas as pd

# Загружаем необходимые таблицы из БД
dl.export('qq85_sttlkofficeinvoice')#Заявки на услуги
dl.export('qq85_sttlkofficepayment')#Оплаченные заявки
dl.export('qq85_stthomeoffice_label')#Лэйблы

# позволяет преобразовать данные из xlsx to csv
def refactor_data(data_path,name_new):
    read_file = pd.read_excel (data_path+".xlsx")
    read_file.to_csv (name_new,
                      index = None,
                      header=True)


refactor_data('qq85_sttlkofficepayment',"payment.csv")
refactor_data('qq85_sttlkofficeinvoice',"invoice.csv")
refactor_data('qq85_stthomeoffice_label',"label.csv")


# В этом блоке мы выполняем подсчет общего количества менеджеров с заявками

invoice=pd.read_csv("invoice.csv")

user_id=invoice["user_id"]
unik_user=user_id.unique()

Manager_df={}

user_count=user_id.tolist()

for i in unik_user:

    Manager_df[i] = user_count.count(i)

ls = list(Manager_df.items())# В этом списке хранится: 1-й элемент- id, 2-й элемент количество заявок на менеджера


df_M=pd.DataFrame(columns=["user_id","count"])


for i in range(len(ls)):
   df_M.loc[i] = ls[i]# в этом датафрейме хранятся по колонкам идентификатор и количество заявок




#В этом блоке мы считаем количество успешных заявок на менеджера

payment=pd.read_csv("payment.csv")
#print(payment.shape)
invoice_id=payment["invoice_id"]# в таблице Пэймент хранятся id не менеджеров, а операций,
# поэтому мы сопоставляем invoice id в таблице payment и id в таблице invoice

invoice_len=invoice["id"]
invoice_id=invoice_id.tolist()


soft_df=pd.DataFrame()

soft_df[["user_id","invoice_id"]]=invoice[["user_id","id"]]

DF_iskl=soft_df[soft_df.user_id !=0]# Исключим нулевые строки в датафрейме
DF_iskl=soft_df.loc[soft_df.invoice_id.isin(invoice_id)==True]# Исключим из копии таблицы invoice все значения,
# которые отсуттсвуют в таблице payment. Так мы получим только те значения, которые превратились в оплаченные заявки


iskl_unik=DF_iskl["user_id"].unique()#Получим уникальных менеджеров в датасете

df_iscl={}



user_count_iscl=DF_iskl["user_id"].tolist()#Переведем все строки с user id в список и посмотрим количество вхождений

for i in iskl_unik:

    df_iscl[i] = user_count_iscl.count(i)

ls_iscl = list(df_iscl.items())


df_end=pd.DataFrame(columns=["user_id","count"])


for i in range(len(ls_iscl)):
   df_end.loc[i] = ls_iscl[i]# В этом датафрейме хранятся значения менеджеров которые совершили успешные сделки(кол-во)





df_M=df_M.loc[df_M.user_id.isin(df_end["user_id"])==True]# Исключим из датасета, где хранятся общее количество заявок
#Всех менеджеров, которые не совершили успешные сделки

df_M=df_M.sort_values(by=["user_id"])


df_end=df_end.sort_values(by=['user_id'])


count=df_M["count"].tolist()# наши датасеты отсортированы и равны, => перенесем колонку с общим количеством в датафрейм
# df_end
df_end["count_all"]=count


convers=[]

for i in range(len(df_end["user_id"])):
    convers.append(df_end["count"][i]/df_end["count_all"][i])# Считаем конверсию

df_end["convers"]=convers# Добавим столбец Конверсии в наш датафрейм

user_end=df_end["user_id"]# Получим список менеджеров


# Получим количество лэйблов
label=pd.read_csv("label.csv")



df_label=label.loc[label.user_id.isin(df_end["user_id"])==True]# Исключим из датафрейма все значения, которые не соответствуют таблице DF_END


user_label=df_label["user_id"].unique()
user_label=sorted(user_label)

df_end=df_end.loc[df_end.user_id.isin(user_label)==True]# сключим из датафрейма все значения, которые не соответствуют таблице df_label



df_label=df_label.sort_values(by=["user_id"])

df_end = df_end.reset_index(drop=True)
df_label = df_label.reset_index(drop=True)



labels=[]
for i in range(len(df_end["user_id"])):#Добавим в список среднее значения всех лэйблов на менеджера
    labels.append([df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label1'].mean(),
                   df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label2'].mean(),
                   df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label3'].mean(),
                   df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label4'].mean(),
                   df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label5'].mean(),
                   df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label6'].mean(),
                   df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label7'].mean(),
                   df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label8'].mean(),
                   df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label9'].mean(),
                   df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label10'].mean(),
                   df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label11'].mean(),
                   df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label12'].mean(),
                   df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label13'].mean(),
                   df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label14'].mean(),
                   df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label15'].mean(),
                   df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label16'].mean(),
                   df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label17'].mean(),
                   df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label18'].mean(),
                   df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label19'].mean(),
                   df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label20'].mean(),
                   df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label21'].mean(),
                   df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label22'].mean(),
                   df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label23'].mean(),
                   df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label24'].mean(),
                   df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label25'].mean(),
                   df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label26'].mean(),
                   df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label27'].mean(),
                   df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label28'].mean(),
                   df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label29'].mean(),
                   ])




col=["label1","label2","label3","label4","label5","label6","label7","label8","label9",'label10',
     "label11","label12","label13","label14","label15","label16","label17","label18","label19",'label20',
     "label21","label22","label23","label24","label25","label26","label27","label28","label29"]



Averange_M=pd.DataFrame(labels,columns=col)

A_user_id=df_end["user_id"].tolist()
Averange_M.insert (loc= 0 , column='user_id', value=A_user_id)

A_convers=[]
for i in range(len(df_end["user_id"])):
    A_convers.append(df_end["count"][i]/df_end["count_all"][i])



Averange_M["convers"]=A_convers

#df_end["label"+str(j+1)][i]=df_label.loc[df_label['user_id'] == df_end["user_id"][i], 'label'+str(j+1)].mean()
# print(df_label.loc[df_label['user_id'] == df_end["user_id"][0], 'label1'].mean())
print(Averange_M.head())
Averange_M.to_excel("Averange_M.xlsx")