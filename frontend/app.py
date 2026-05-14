from flask import Flask, render_template
import requests

app = Flask(__name__)

@app.route("/")
def home():
    response = requests.get("http://api:8000/drivers")
    drivers = response.json()

    return render_template("index.html", drivers=drivers)

if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True, port=5000)