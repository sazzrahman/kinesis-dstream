from flask import Flask, request, jsonify
from functools import wraps
import os

app = Flask(__name__)


def authorize(func):
    """custom header authorization decorator

    Args:
        func (_type_): any route function

    Returns:
        _type_: the function if the header is valid
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        headers = request.headers
        auth = headers.get("x-api-key")
        if auth:
            if auth == os.getenv("API_KEY"):
                return func(*args, **kwargs)
            else:
                return jsonify({"message": "Invalid API KEY"}), 401

        else:
            return jsonify({"message": "Must provide x-api-key:token"}), 401
    return wrapper


@app.route("/hello", methods=["GET"])
@authorize
def hello():
    return jsonify({"message": f"Hello Authorized"}), 200


if __name__ == "__main__":
    app.run(debug=bool(int(os.getenv("DEBUG"))))
