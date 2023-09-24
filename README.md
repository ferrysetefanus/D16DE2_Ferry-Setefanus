# Assignment Day 16 Dibimbing Data Engineering Batch 2

## Exercise
Implement your previous task, but in cloud, but now it’s just simple word count :)
Use GCP services :
 - Composer
 - Dataproc (Pyspark)
(Bukan MapReduce Hadoop seperti di contoh video penjelasan)

## Apa saja yang dilakukan
1. Mengubah operator yang digunakan
   ![operator](https://github.com/ferrysetefanus/D16DE2_Ferry-Setefanus/blob/main/img/operator.jpg)
   
2. Membuat konfigurasi untuk menghubungkan script pyspark ke airflow dan cluster dataproc
   ![job](https://github.com/ferrysetefanus/D16DE2_Ferry-Setefanus/blob/main/img/job.jpg)
      
3. Menjalankan DAG menggunakan airflow
   ![dag](https://github.com/ferrysetefanus/D16DE2_Ferry-Setefanus/blob/main/img/dag.jpg)
   
4. Cek semua task yang sudah dijalankan
   ![task](https://github.com/ferrysetefanus/D16DE2_Ferry-Setefanus/blob/main/img/airflow.jpg)
   
5. Cek hasil wordcount di cluster dataproc
   ![result](https://github.com/ferrysetefanus/D16DE2_Ferry-Setefanus/blob/main/img/result.jpg) 


