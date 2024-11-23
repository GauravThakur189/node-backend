from fastapi import FastAPI, Form
from fastapi import FastAPI, Request
from typing import List, Dict

app = FastAPI()

@app.get('/')
def read_root():
    return {'Ping': 'Pong'}

@app.get('/pipelines/parse')
def parse_pipeline(pipeline: str = Form(...)):
    return {'status': 'parsed'}
def is_dag(nodes: List[Dict], edges: List[Dict]) -> bool:
    from collections import defaultdict, deque
    graph = defaultdict(list)
    in_degree = defaultdict(int)

    # Build graph and in-degree counts
    for edge in edges:
        graph[edge['source']].append(edge['target'])
        in_degree[edge['target']] += 1
    
    # Perform a topological sort
    queue = deque(node['id'] for node in nodes if in_degree[node['id']] == 0)
    visited = 0

    while queue:
        current = queue.popleft()
        visited += 1
        for neighbor in graph[current]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    return visited == len(nodes)

@app.post("/pipelines/parse")
async def parse_pipeline(request: Request):
    data = await request.json()
    nodes = data.get('nodes', [])
    edges = data.get('edges', [])

    num_nodes = len(nodes)
    num_edges = len(edges)
    dag = is_dag(nodes, edges)

    return {"num_nodes": num_nodes, "num_edges": num_edges, "is_dag": dag}
