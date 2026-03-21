from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from data import generate_order
import uvicorn
import json

app = FastAPI()
templates = Jinja2Templates(directory="templates")

@app.get("/")
def home(request: Request):
    return templates.TemplateResponse("home.html", {"request": request})

@app.get("/order")
def place_order(request: Request):
    order = generate_order()
    print(f"Order created: {order['order_id']} | Restaurant: {order['restaurant_id']} | Total: ${order['total_amount']}")
    return templates.TemplateResponse("confirmation.html", {"request": request})


if __name__ == "__main__":
   
    uvicorn.run(app, host="0.0.0.0", port=8000)
 