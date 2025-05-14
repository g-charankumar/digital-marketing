from flask import Flask, request, jsonify
import pandas as pd

app = Flask(__name__)

# Load the dataset
def load_dataset():
    return pd.read_csv("restro.csv")

@app.route('/get_data', methods=['GET'])
def get_data():
    # Get the search keyword from query parameters
    keyword = request.args.get('keyword', '')

    # Load dataset
    df = load_dataset()

    # Filter dataset based on the keyword
    filtered_data = df[df.apply(lambda row: row.astype(str).str.contains(keyword, case=False).any(), axis=1)]

    # Return filtered dataset as JSON
    return jsonify(filtered_data.to_dict(orient='records'))

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
