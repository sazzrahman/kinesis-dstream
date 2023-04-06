from flask import Flask, request, jsonify
from functools import wraps
import os
import subprocess
import sys
import re

app = Flask(__name__)


def print_line(text, pattern=None):
    lines = text.split("\\n")
    out_list = []

    for line in lines:
        if pattern in re.findall(pattern, line):
            spl_line = line.split(" ")
            pid = spl_line[3]
            user = spl_line[4]
            time = spl_line[10]
            command = spl_line[11:]
            out_list.append(dict(
                pid=pid,
                user=user,
                time=time,
                command=command
            ))
    return out_list


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


@app.route("/", methods=["GET"])
def home():
    payload = request.get_json()
    if payload.get("job_name"):
        return jsonify({"message": f"Server up and running"}), 200


@app.route("/start", methods=["POST"])
@authorize
def start_job():

    payload = request.get_json()
    if payload:
        if payload.get("job_name"):
            job_name = payload.get('job_name')

            subprocess.Popen(
                [sys.executable, f"{job_name}.py"], stdout=subprocess.PIPE)

            output = subprocess.Popen(
                ["ps", "-A", "|", "grep", f"{job_name}"], stdout=subprocess.PIPE, shell=True).communicate()

            output = print_line(str(output), pattern=job_name)

            return jsonify({"message": f"{output}"}), 200
        else:
            return jsonify({"message": "Must provide job_name"}), 400
    else:
        return jsonify({"message": "Must provide arguments in payload"}), 400


@ app.route("/stop", methods=["POST"])
@ authorize
def stop_job():
    payload = request.get_json()
    if payload.get("pid"):
        pid = payload.get('pid')

        status = subprocess.run(
            [f"kill", "-9", f"{pid}"], capture_output=True)

        return jsonify({"message": f"Job {status} ended"}), 200
    else:
        return jsonify({"message": "Must provide job_name"}), 400


@ app.route("/check", methods=["POST"])
@ authorize
def check_job():

    payload = request.get_json()
    if payload.get("job_name"):
        job_name = payload.get('job_name')

        output = subprocess.Popen(
            ["ps", "-A", "|", "grep", f"{job_name}"], stdout=subprocess.PIPE, shell=True).communicate()

        status = print_line(str(output), pattern=job_name)

        return jsonify({"current_status": status}), 200
    else:
        return jsonify({"message": "Must provide job_name"}), 400


if __name__ == "__main__":
    app.run(debug=bool(int(os.getenv("DEBUG"))))
