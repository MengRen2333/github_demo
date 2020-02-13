# Frount-End

flask + Tableau

## 1. Website
[ComfortLive](http://predictionsanalytics.xyz/)

## 2. Create instance
* Create a special security group for webpage instance.
[image](http://github.com/MengRen2333/living-recommendation/raw/master/images/security.png)
* Create a new instance, t2micro, for webpage runing.

## 3. Run python file through flask 
* Log in to your instance
* Install flask
```
pip3 install flask
```
* Set up flask
```
export FLASK_APP=<your own python file>
```
* Run python file
```
sudo python3 -m http.server 80
```

## 4. Set up web page on namecheap domain

Buy a domain --> Dashboard --> Manage --> Advanced DNS --> add new record -->
[image](http://github.com/MengRen2333/living-recommendation/raw/master/images/namecheap.png)



