from flask import Flask, render_template, request, redirect, url_for, jsonify
 # initialize flask
app= Flask(__name__)
@app.route('/')
def main():
      return render_template("index.html")

if __name__ == "main":

      # update running app

      #app.config['DEBUG'] = True

      # point from domain name to ec2

      app.run(host="0.0.0.0", port=80)
