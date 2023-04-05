from flask import Flask
import os

app = Flask(__name__)


@app.route("/hello", methods=["GET"])
def hello():
    return "Hello World!"


if __name__ == "__main__":
    app.run(debug=bool(int(os.getenv("DEBUG"))))
