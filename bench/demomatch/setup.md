# Building the MySQL database

```
sudo docker run --name mysql-conv -v /scratch/jack/fastdb/bench/demomatch/usr/local/var/mysql:/var/lib/mysql -d mysql:5.7
```

Log in and run:

```
GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' IDENTIFIED BY 'password' WITH GRANT OPTION;
```

# Building the postgresql database

```
sudo docker run --name post-conv --link mysql-conv:mysql -e POSTGRES_PASSWORD=password -d postgres
```

Log in and run:

```
apt-get update
apt-get -y install libssl1.0.2
apt-get -y install pgloader

createdb swing_components_ProgressBarDemo
pgloader mysql://root:password@172.17.0.2/swing_components_ProgressBarDemo pgsql:///swing_components_ProgressBarDemo 
```
