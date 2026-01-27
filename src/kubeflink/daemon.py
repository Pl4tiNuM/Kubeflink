import logging, threading
from flask import Flask, request, jsonify

from kubeflink import KubeFlink

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(levelname)s]: %(message)s')

app = Flask(__name__)

kf = KubeFlink()

# start the background run loop so REST API can be served concurrently
threading.Thread(target=kf.run, daemon=True).start()


@app.route('/get_tms', methods=['POST'])
def get_tms():
    """
    POST /list_svcs
    - If JSON body contains {"svcs": [...]} where each item is a dict with at least "name",
      those services will be filtered and returned.
    - Otherwise, if KubeCollector exposes a list_services() method, it will be used.
    Response: {"flink_services": ["name1", "name2", ...]}
    """
    data = request.get_json(silent=True) or {}

    svc_objs = []
    if isinstance(data.get('svcs'), list):
        for s in data['svcs']:
            if isinstance(s, dict):
                # create a tiny object with .name attribute for compatibility
                svc_objs.append(type('Svc', (), {'name': s.get('name', '')})())
            elif hasattr(s, 'name'):
                svc_objs.append(s)
            else:
                # fallback to string representation
                svc_objs.append(type('Svc', (), {'name': str(s)})())
    else:
        # try to obtain services from KubeCollector
        if hasattr(kf.kc, 'list_services'):
            raw = kf.kc.list_services()
            if isinstance(raw, list):
                svc_objs = raw
            else:
                try:
                    svc_objs = list(raw)
                except Exception:
                    return jsonify({"error": "KubeCollector.list_services() did not return a list"}), 500
        else:
            return jsonify({"error": "No services provided in request and KubeCollector has no list_services()"}), 400

    flink_svcs = kf.filter_flink_svc(svc_objs)
    return jsonify({"flink_services": [s.name for s in flink_svcs]}), 200


if __name__ == '__main__':
    # Listen on all interfaces by default; change port as needed
    app.run(host='0.0.0.0', port=1337)