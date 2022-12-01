from flask import Flask
from flask import render_template
from flask import request
from flask import redirect, url_for
from search import Search

# creates flask app to run
app = Flask(__name__)

search = Search()
# specifies route for flask app to run on
@app.route('/', methods=['GET', 'POST'])
def home():
    global search
    search = Search()
    if request.method == 'POST':
        query = request.form.get("search")
        return redirect(url_for('results', query=query))
    return render_template('search-engine.html')


@app.route('/results/<string:query>', methods=['GET', 'POST'])
def results(query):
    sites, delay = search.document_retrieval(query.split())
    return render_template('results.html', keyterm=query, sites=sites, delay=delay)


if __name__ == '__main__':
    # localhost:5000/
    app.run(debug=True)