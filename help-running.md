# How to Run

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Run Services (in separate terminals)

**Terminal 1: Broker**

```bash
python -m server.broker
```

**Terminal 2: Frontend**

```bash
python -m frontend.app
```

**Terminal 3: Simulator**

```bash
python -m workers.simulator
```

### 3. View in Browser

- URL: [http://127.0.0.1:5000](http://127.0.0.1:5000)

