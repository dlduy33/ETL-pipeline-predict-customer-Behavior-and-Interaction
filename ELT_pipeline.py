from pyspark.sql.session import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import StructType,StructField,StringType
import os

spark = SparkSession.builder.config('spark.executor.memory', '14g')\
							.config('spark.executor.cores', 10)\
							.config('spark.jars.packages','mysql:mysql-connector-java:8.0.29').getOrCreate()

# Clean Data
def clean_data_content(path_content):
	print('--------------------')
	print('Read data from Kafka')
	print('--------------------')
	df = spark.read.json(path_content)
	print('--------------------')
	print('Selecting all values in _source.')
	print('--------------------')
	df = df.select('_source.*')
	print('--------------------')
	print('ETL Start')
	print('--------------------')
	df = df.withColumn('Type',
		when((col('AppName') == 'CHANNEL')| (col('AppName') == 'DSHD')| (col('AppName') == 'KPLUS')| (col('AppName') == 'KPlus'), 'Truyền_Hình')
		.when((col('AppName') == 'VOD_RES')| (col('AppName') == 'FIMS')| (col('AppName') == 'BHD')| (col('AppName') == 'DANET'), 'Phim_Truyện')
		.when((col('AppName') == 'RELAX'), 'Giải_Trí')
		.when((col('AppName') == 'CHILD'), 'Thiếu_Nhi')
		.when((col('AppName') == 'SPORT'), 'Thể_Thao')
		.otherwise('Error'))
	print('--------------------')
	print('Cleaning Error & Finding expected values')
	print('--------------------')
	df = df.select('Contract', 'Type', 'TotalDuration')
	df = df.filter((df.Type != 'Error'))
	df = df.filter((df.TotalDuration >= 0) | (df.Contract != '0'))
	return df

# Take out the type that customers view the most
def calculate_most_watch(cleaned_data_content):
	windowSpec = Window.partitionBy('Contract').orderBy(desc('TotalDuration'))
	most_watch = cleaned_data_content.withColumn('rank', rank().over(windowSpec))
	most_watch = most_watch.filter(col('rank') == 1)
	most_watch = most_watch.withColumnRenamed('Type', 'Most_Watch')
	most_watch = most_watch.select('Contract', 'Most_Watch')
	return most_watch

# Take out the types that the customer has opened
def calculate_taste(content_result):
	taste = content_result.withColumn('Giải_Trí', when(col('Giải_Trí').isNotNull(), 'G.Trí'))
	taste = taste.withColumn('Phim_Truyện', when(col('Phim_Truyện').isNotNull(), 'P.Truyện'))
	taste = taste.withColumn('Thiếu_Nhi', when(col('Thiếu_Nhi').isNotNull(), 'T.Nhi'))
	taste = taste.withColumn('Thể_Thao', when(col('Thể_Thao').isNotNull(), 'T.Thao'))
	taste = taste.withColumn('Truyền_Hình', when(col('Truyền_Hình').isNotNull(), 'T.Hình'))
	taste = taste.withColumn('Taste', concat_ws(', ', *[col for col in taste.columns if col != 'Contract']))
	taste = taste.select('Contract', 'Taste')
	return taste

'''Using IQR (interquartile range) to decide the usage frequency of the user.
	Divide total duration to 3 parts:
		25%(q1) < duration: lower
		25%(q1) =< duration =< 75%(q3): middle
		duration < 75%(q3): upper
		result = {'q1': 10939.0, 'q3': 324524.0}
http://thongke.cesti.gov.vn/dich-vu-thong-ke/tai-lieu-phan-tich-thong-ke/845-thong-ke-mo-ta-trong-nghien-cuu-dai-luong-do-phan-tan'''
def calculate_iqr(cleaned_data_content):
	bounds = {
		c: dict(
			zip(['q1', 'q3'], cleaned_data_content.approxQuantile(c, [0.25, 0.75], 0)))
		for c,d in zip(cleaned_data_content.columns, cleaned_data_content.dtypes) if d[1] == 'bigint'
	}
	q1 = bounds['TotalDuration']['q1']
	q3 = bounds['TotalDuration']['q3']
	iqr_type = cleaned_data_content.withColumn('IQR_Type', when(col('TotalDuration') < q1, 'lower')
		.when(col('TotalDuration') > q3, 'upper').otherwise('middle'))
	iqr_type = iqr_type.select('Contract', 'IQR_Type')
	return iqr_type

# Count the numner of days customers open in the month
def calculate_activeness(path_content, files):
	schema = StructType([StructField('Contract', StringType(), True)])
	activeness_form = spark.createDataFrame([], schema=schema)
	for file in files:
		print(file)
		cleaned_data_content = spark.read.json(path_content+f'\\{file}')
		cleaned_data_content = cleaned_data_content.select('_source.*')
		cleaned_data_content = cleaned_data_content.select('Contract').dropDuplicates(['Contract'])
		activeness_form = activeness_form.union(cleaned_data_content)
	activeness = activeness_form.filter(col('Contract') != '0').select('Contract')
	activeness = activeness.withColumn('Activeness', lit(1))
	activeness = activeness.groupBy('Contract').sum('Activeness')
	activeness = activeness.withColumnRenamed('sum(Activeness)', 'Activeness')
	activeness = activeness.withColumn('Activeness', activeness.Activeness.cast('int'))
	return activeness

'''User behavior metrics based on 'iqr_type' & 'activeness(days)'
	low:
		- Activeness <= 15days & iqr_type = lower
	medium:
		- 15days < Activeness & iqr_type = lower
		- Activeness <= 10days & iqr_type = middle
		- Activeness <= 10days & iqr_type = upper
	high:
		- 10days <= Activeness & iqr_type = middle
		- 10days <= Activeness & iqr_type = upper '''
def calculate_clinginess(iqr_type, activeness):
	iqr_activeness_group = iqr_type.join(activeness, on='Contract', how='outer')
	clinginess = iqr_activeness_group.withColumn('Clinginess', when((col('Activeness') <= 15) & (col('IQR_Type') == 'lower'), 'low')
		.when((15 < col('Activeness')) & (col('IQR_Type') == 'lower'), 'medium')
		.when((col('Activeness') <= 10) & (col('IQR_Type') == 'middle'), 'medium')
		.when((col('Activeness') <= 10) & (col('IQR_Type') == 'upper'), 'medium')
		.when((10 < col('Activeness')) & (col('IQR_Type') == 'middle'), 'high')
		.when((10 < col('Activeness')) & (col('IQR_Type') == 'upper'), 'high')
		)
	clinginess = clinginess.select('Contract', 'Clinginess')
	return clinginess

# Cleaning data search
def clean_data_search(path, files):
	schema = StructType([StructField('user_id', StringType(), True),
						StructField('keyword', StringType(), True)])
	df_form = spark.createDataFrame([], schema=schema)
	for file in files:
		df = spark.read.parquet(path+f'\\{file}')
		df = df.filter(((df.keyword.isNotNull()) & (df.category == 'enter') & (df.user_id.isNotNull()))).select('user_id', 'keyword')
		df_form = df_form.union(df)
	return df_form

# Finding most search
def calculate_most_search(cleaned_data):
	most_search = cleaned_data.groupBy('user_id', 'keyword').count()
	windowSpec = Window.partitionBy('user_id').orderBy(desc('count'))
	most_search = most_search.withColumn('rank', rank().over(windowSpec))
	most_search = most_search.filter(col('rank') == 1)
	most_search = most_search.filter(col('keyword').isNotNull()).select('user_id', 'keyword')
	most_search = most_search.withColumnRenamed('keyword', 'Most_Search')
	return most_search

# Finding category each month
def calculate_category(most_search):
	category_dict = spark.read.csv(r'D:\Data Engineer\BigData\Watch_Search\map_table.csv', header=True)
	category = category_dict.join(most_search, category_dict.keyword == most_search.Most_Search, 'full')
	category = category.fillna('Other').select('user_id', 'Category')
	category = category.groupBy('user_id', 'Category').sum()
	return category

# Finding the change of behavior
def calculate_behavior(category_june, category_july):
	category = category_june.select(col('Category').alias('Category_1st'), col('user_id'))\
			.join(category_july.select(col('Category').alias('Category_2nd'), col('user_id')), on='user_id', how='outer')
	category = category.fillna('Other')
	behavior_category = category.withColumn('Behavior_Change', when(col('Category_1st') != col('Category_2nd'), 'Changed').otherwise('Unchanged'))
	explain_changed = behavior_category.withColumn('Explain_Changed', 
					when(col('Behavior_Change') == 'Changed', concat_ws('>', behavior_category.Category_1st, behavior_category.Category_2nd)).otherwise('Unchanged'))
	behavior_change = explain_changed.select('user_id', 'Behavior_Change', 'Explain_Changed')
	return behavior_change

# Finding top 10 trending search
def calculate_trending(cleaned_data):
	trending = cleaned_data.select('keyword')
	trending = trending.groupBy('keyword').count().orderBy('count', ascending=False)
	trending = trending.limit(10)
	return trending

# Import data to database
def import_data_to_mysql(data, table_name):
    url = 'jdbc:mysql://' + 'localhost' + ':' + '3306' + '/' + 'data_engineer'
    driver = "com.mysql.cj.jdbc.Driver"
    user = 'root'
    password = ''
    data.write.format('jdbc').option('url',url).option('driver',driver).option('dbtable',f'{table_name}')\
							.option('user',user).option('password',password).mode('append').save()
    return print('Data imported successfully')

def main():
	print('------------------------')
	print('Cleaning Data of Content')
	print('------------------------')
	path_content = r'D:\Data Engineer\BigData\Watch_Search\log_content'
	files = os.listdir(path_content)
	file01 = files[0]
	cleaned_data_content_form = clean_data_content(path_content + f'\\{file01}') #Take 1st cleaned_data_content as a form for the rest
	for file in files[1:]:
		file_path_content = path_content + f'\\{file}'
		print(f'Finished Processing {file}')
		cleaned_data_content = clean_data_content(file_path_content)
		cleaned_data_content_form = cleaned_data_content_form.union(cleaned_data_content)
	cleaned_data_content = cleaned_data_content_form.groupBy('Contract', 'Type').sum()
	cleaned_data_content = cleaned_data_content.withColumnRenamed('sum(TotalDuration)', 'TotalDuration')
	print('------------------------')
	print('Finding the Most Watch')
	print('------------------------')
	most_watched = calculate_most_watch(cleaned_data_content)
	print('------------------------')
	print('Finding the InterQuartile Range')
	print('------------------------')
	iqr_type = calculate_iqr(cleaned_data_content)
	print('------------------------')
	print('Finding the Activeness')
	print('------------------------')
	activeness = calculate_activeness(path_content, files)
	print('------------------------')
	print('Finding the Clinginess')
	print('------------------------')
	clinginess = calculate_clinginess(iqr_type, activeness)
	print('------------------------')
	print('Pivoting Data')
	print('------------------------')
	content_result = cleaned_data_content.groupBy('Contract').pivot('Type').sum('TotalDuration')
	print('------------------------')
	print('Finding the Taste')
	print('------------------------')
	taste = calculate_taste(content_result)
	print('------------------------')
	print('Joining Content Data')
	print('------------------------')
	content_final_result = content_result.join(most_watched, on='Contract', how='outer')\
		.join(iqr_type, on='Contract', how='outer')\
			.join(activeness, on='Contract', how='outer')\
				.join(clinginess, on='Contract', how='outer')\
					.join(taste, on='Contract', how='outer')
	print('------------------------')
	print('Cleaning Data of Search')
	print('------------------------')
	path_search = r'D:\Data Engineer\BigData\Watch_Search\log_search'
	files_june = os.listdir(path_search)[:14]
	files_july = os.listdir(path_search)[14:]
	cleaned_data_june = clean_data_search(path_search, files_june)
	cleaned_data_july = clean_data_search(path_search, files_july)
	print('------------------------')
	print('Finding the Most Search')
	print('------------------------')
	most_search_june = calculate_most_search(cleaned_data_june)
	most_search_july = calculate_most_search(cleaned_data_july)
	print('------------------------')
	print('Finding Category each month')
	print('------------------------')
	category_june = calculate_category(most_search_june)
	category_july = calculate_category(most_search_july)
	print('------------------------')
	print('Finding the Behavior Change')
	print('------------------------')
	behavior_change = calculate_behavior(category_june, category_july)
	print('------------------------')
	print('Finding top 10 Search')
	print('------------------------')
	trending_june = calculate_trending(cleaned_data_june)
	trending_july = calculate_trending(cleaned_data_july)
	print('------------------------')
	print('Joining Search Data')
	print('------------------------')
	search_final_result = cleaned_data_june.select(col('user_id'), col('keyword').alias('keyword_june'))\
		.join(cleaned_data_july.select(col('user_id'), col('keyword').alias('keyword_july')), on='user_id', how='outer')\
			.join(most_search_june.select(col('user_id'), col('Most_Search').alias('Most_Search_june')), on='user_id', how='outer')\
				.join(most_search_july.select(col('user_id'), col('Most_Search').alias('Most_Search_july')), on='user_id', how='outer')\
					.join(category_june.select(col('user_id'), col('Category').alias('Category_june')), on='user_id', how='outer')\
						.join(category_july.select(col('user_id'), col('Category').alias('Category_july')), on='user_id', how='outer')\
							.join(behavior_change, on='user_id', how='outer')
	print('------------------------')
	print('Joining both of Content & Search data')
	print('------------------------')
	content_final_result = content_final_result.withColumn('join', lit(1)) # There is no complete data so must create a common columns to fake data joining them at all
	search_final_result = search_final_result.withColumn('join', lit(1))
	trending_june = trending_june.withColumn('join', lit(1))
	trending_july = trending_july.withColumn('join', lit(1))
	final_result = content_final_result.join(search_final_result, on='join', how='outer')\
				.join(trending_june.select(col('join'), col('keyword').alias('Top_10_june'), col('count').alias('Search_Number_june')), on='join', how='outer')\
					.join(trending_july.select(col('join'), col('keyword').alias('Top_10_july'), col('count').alias('Search_Number_july')), on='join', how='outer')
	final_result = final_result.drop('join', 'user_id')
	print('------------------------')
	print('Importing data to mysql')
	print('------------------------')
	table_name = input("What's table name? ").lower().strip()
	import_data_to_mysql(final_result, table_name)
	print('------------------------')
	print('ETL Successfully')
	print('------------------------')

main()