import shap
import lime.lime_text
import matplotlib.pyplot as plt
import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.express as px
import logging  
import tokenizers
logging.info("Initializing XAI visualization module...")

explainer_lime = lime.lime_text.LimeTextExplainer(class_names=['Fake', 'Real'])
explainer_shap = shap.Explainer(model)

def explain_with_lime(text):
    exp = explainer_lime.explain_instance(text, lambda x: [classify_news(t) for t in x], num_features=10)
    return exp.show_in_notebook()

def explain_with_shap(text):
    inputs = tokenizer(text, return_tensors='pt', truncation=True, padding=True, max_length=512)
    shap_values = explainer_shap(inputs)
    shap.plots.text(shap_values)

logging.info("XAI Visualization module loaded.")