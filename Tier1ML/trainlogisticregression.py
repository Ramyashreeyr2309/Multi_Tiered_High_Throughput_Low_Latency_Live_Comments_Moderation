from river import compose
from river import preprocessing
from river import linear_model
from river import metrics
from river import stream
from sentence_transformers import SentenceTransformer

# 1. Path to your custom dataset
csv_path = 'trainsmall.csv'

# 2. Build an online text processing pipeline
# It takes the 'comment_text' field, tokenizes it into a Bag of Words,
# and passes the streaming vectors into the Logistic Regression model.
model =  compose.Pipeline(
    preprocessing.StandardScaler(),
    linear_model.LogisticRegression()
)

# 3. Track metrics suited for imbalanced moderation tasks
metric = metrics.ROCAUC()
# 4. Stream data line-by-line from the CSV
# target='is_toxic' tells River which column holds the 0/1 labels

model1 = SentenceTransformer(
    "all-MiniLM-L6-v2", backend="onnx", model_kwargs={"file_name": "onnx/model.onnx"}
)


with open(csv_path, mode="r", encoding="utf-8", errors="ignore") as f:
    comment_stream = stream.iter_csv(
        f, 
        target='is_toxic',
        converters={'is_toxic': int}  # Explicitly cast labels to integers
    )
    for i, (x,y) in enumerate(comment_stream):
        # print(i)
        # print(x['comment_text'],y)
        embeddings = model1.encode(x['comment_text'])
        v = {i: float(val) for i, val in enumerate(embeddings)}

        # Predict the probability of toxicity BEFORE updating the model
        y_pred_proba = model.predict_proba_one(v)
        
        # Update our performance tracking metric
        if y_pred_proba:
            # River returns a dictionary of probabilities: {0: prob_clean, 1: prob_toxic}
            metric.update(y, y_pred_proba.get(1, 0.0))
            
        # Online training: update weights using this single comment instance
        model.learn_one(v, y)
        
        # Print metrics every 500 lines to track progress
        if i % 500 == 0 and i > 0:
            print(f"Comments processed: {i:5d} | Cumulative ROC AUC: {metric.get():.4f}")

print("\n--- Final Stream Summary ---")
print(f"Total Comments Processed: {i+1}")
print(f"Final Model ROC AUC: {metric.get():.4f}")

# Example of real-time manual testing
test_comment = "This framework is very helpful. thank you."
test_embeddings = model1.encode(test_comment)
test_v = {i: val for i, val in enumerate(test_embeddings)}
prob = model.predict_proba_one(test_v).get(1, 0.0)
print(f"\nLive Test Comment: '{test_comment}'")
print(f"Predicted Toxicity Probability: {prob:.2%}")
