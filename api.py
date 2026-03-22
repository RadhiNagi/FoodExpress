from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from data import generate_order
from connection import send_to_event_hub
import json

app = FastAPI()
templates = Jinja2Templates(directory="templates")


@app.get("/")
def home(request: Request):
    return templates.TemplateResponse("home.html", {"request": request})


@app.get("/order")
def place_order(request: Request):
    order = generate_order()
    result = send_to_event_hub(order)
    print(f"Order: {order['order_id'][:8]} | Restaurant: {order['restaurant_id']} | Total: ${order['total_amount']} | Event Hub: {result}")
    return templates.TemplateResponse("confirmation.html", {"request": request})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)