import subprocess

graph_path = r"E:\Big Data\Summer Project\myGraph.xml"

cmd = [
    "gpt", graph_path,

]

subprocess.run(cmd, check=True)
