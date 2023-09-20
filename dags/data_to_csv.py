import pandas as pd
from io import StringIO
from upload_to_s3 import s3_obj

def data_to_csv(data_list, extention):
    df = pd.DataFrame(data_list)
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3=s3_obj()
    s3.put_object(Bucket='immostudy-temp',Key=f"csv_files/{extention}",Body=csv_buffer.getvalue())

df =pd.read_csv("/home/bo/study/becode/Immo_Eliza_pipeline/working/utls/Belgium_Postalcode.csv")
csv_buffer = StringIO()
df.to_csv(csv_buffer, index=False)
s3=s3_obj()
s3.put_object(Bucket='immostudy-temp',Key=f"csv_files/test.csv",Body=csv_buffer.getvalue())