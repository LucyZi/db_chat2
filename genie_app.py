import os
import json
import time
import requests
import traceback
import certifi
from flask import Flask, request, jsonify, render_template_string

# --- 配置 ---
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
GENIE_SPACE_ID = os.getenv("GENIE_SPACE_ID")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

# --- 完整的聊天机器人UI模板 (终极版) ---
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Part D and Part B Spending by Drug</title>
    <script src="https://unpkg.com/lucide@latest"></script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        :root { --bg-color: #ffffff; --text-color: #1a1a1a; --border-color: #e0e0e0; --placeholder-color: #6b7280; --bot-bg: #f7f7f7; --accent-color: #6366f1; }
        
        body { 
            margin: 0; 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif; 
            background-color: var(--bg-color); 
            color: var(--text-color); 
        }

        .chat-container { 
            width: 100%; 
            max-width: 800px; 
            height: 90vh; 
            max-height: 800px; 
            display: flex; 
            flex-direction: column; 
            border: 1px solid var(--border-color); 
            border-radius: 12px; 
            box-shadow: 0 4px 12px rgba(0,0,0,0.05); 
            overflow: hidden;
            position: absolute;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
        }
        
        .chat-header { 
            padding: 1rem; 
            border-bottom: 1px solid var(--border-color); 
            font-weight: 600; 
            font-size: 1.1rem;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        .new-chat-btn {
            background: none;
            border: none;
            cursor: pointer;
            color: var(--placeholder-color);
            padding: 4px;
            border-radius: 6px;
            display: flex;
            align-items: center;
            justify-content: center;
        }
        .new-chat-btn:hover {
            background-color: var(--bot-bg);
            color: var(--text-color);
        }

        .chat-messages { flex-grow: 1; overflow-y: auto; padding: 1.5rem; display: flex; flex-direction: column; }
        .welcome-screen { text-align: center; margin: auto; }
        .welcome-icon { width: 60px; height: 60px; background: linear-gradient(135deg, #a78bfa, #6366f1); border-radius: 12px; display: flex; align-items: center; justify-content: center; margin: 0 auto 1rem; color: white; }
        .welcome-screen h2 { font-size: 1.5rem; margin-bottom: 0.5rem; }
        .sample-questions { margin-top: 2rem; display: flex; flex-direction: column; gap: 0.75rem; align-items: flex-start; margin-left: auto; margin-right: auto; max-width: 90%; }
        .sample-question { padding: 0.6rem 1rem; border: 1px solid var(--border-color); border-radius: 8px; cursor: pointer; transition: background-color 0.2s; font-size: 0.9rem; display: flex; align-items: center; gap: 0.5rem; text-align: left; }
        .sample-question:hover { background-color: #f9fafb; }
        .message { max-width: 85%; padding: 0.75rem 1.25rem; border-radius: 18px; margin-bottom: 1rem; line-height: 1.5; white-space: pre-wrap; font-family: 'Menlo', 'Consolas', monospace; font-size: 0.9rem; }
        .user-message { background-color: var(--accent-color); color: white; align-self: flex-end; border-bottom-right-radius: 4px; }
        .bot-message { background-color: var(--bot-bg); color: var(--text-color); align-self: flex-start; border-bottom-left-radius: 4px; }
        .chart-container-wrapper { align-self: flex-start; width: 100%; }
        .chart-container { background-color: var(--bot-bg); padding: 1rem; border-radius: 18px; margin-bottom: 1rem; width: 100%; box-sizing: border-box; }
        .typing-indicator { align-self: flex-start; display: flex; gap: 4px; padding: 0.75rem 1.25rem; }
        .typing-indicator span { width: 8px; height: 8px; background-color: #ccc; border-radius: 50%; animation: bounce 1s infinite; }
        .typing-indicator span:nth-child(2) { animation-delay: 0.2s; }
        .typing-indicator span:nth-child(3) { animation-delay: 0.4s; }
        @keyframes bounce { 0%, 80%, 100% { transform: scale(0); } 40% { transform: scale(1); } }
        .chat-input-area { border-top: 1px solid var(--border-color); padding: 1rem; display: flex; gap: 0.5rem; align-items: flex-start; }
        .chat-input-area textarea { 
            flex-grow: 1; 
            border: 1px solid var(--border-color); 
            border-radius: 8px; 
            padding: 0.75rem; 
            font-size: 1rem; 
            resize: none; 
            font-family: inherit;
            max-height: 150px;
            overflow-y: auto;
        }
        .chat-input-area textarea:focus { outline: none; border-color: var(--accent-color); }
        .chat-input-area button { border: none; background-color: var(--accent-color); color: white; padding: 0.75rem 1rem; border-radius: 8px; cursor: pointer; display: flex; align-items: center; justify-content: center; height: fit-content; }
    </style>
</head>
<body>
    <div class="chat-container">
        <div class="chat-header">
            <span>Part D and Part B Spending by Drug</span>
            <button class="new-chat-btn" onclick="window.location.reload()" title="New Chat">
                <i data-lucide="plus-circle"></i>
            </button>
        </div>
        <div class="chat-messages" id="chat-messages">
            <div class="welcome-screen" id="welcome-screen">
                <div class="welcome-icon"><i data-lucide="bot"></i></div>
                <h2>Part D and Part B Spending by Drug</h2>
                <!-- --- HTML 最终修复: 恢复示例问题 --- -->
                <div class="sample-questions">
                    <div class="sample-question" onclick="askSample(this)">
                        <i data-lucide="table-2" width="16"></i>
                        <span>What is the distribution of total spending (tot_spndng) for the drugs?</span>
                    </div>
                    <div class="sample-question" onclick="askSample(this)">
                        <i data-lucide="hash" width="16"></i>
                        <span>What is the monthly timeseries distribution of total claims (tot_clms) for the drugs?</span>
                    </div>
                    <div class="sample-question" onclick="askSample(this)">
                        <i data-lucide="pie-chart" width="16"></i>
                        <span>What is the distribution of drug brands (brnd_name) in the dataset?</span>
                    </div>
                    <div class="sample-question" onclick="askSample(this)">
                        <i data-lucide="trending-up" width="16"></i>
                        <span>What tables are there and how are they connected? Give me a short summary.</span>
                    </div>
                </div>
            </div>
        </div>
        <div class="chat-input-area">
            <textarea id="userInput" placeholder="Ask your question..." rows="1" onkeydown="handleEnter(event)"></textarea>
            <button id="sendButton" onclick="sendMessage()"><i data-lucide="send"></i></button>
        </div>
    </div>

    <script>
        lucide.createIcons();
        const chatMessages = document.getElementById('chat-messages');
        const userInput = document.getElementById('userInput');
        const welcomeScreen = document.getElementById('welcome-screen');

        userInput.addEventListener('input', function () {
            this.style.height = 'auto';
            this.style.height = (this.scrollHeight) + 'px';
        });

        function askSample(element) {
            const question = element.querySelector('span').innerText;
            userInput.value = question;
            // 触发 input 事件来调整高度
            userInput.dispatchEvent(new Event('input', { bubbles: true }));
            sendMessage();
        }

        function handleEnter(event) {
            if (event.key === 'Enter' && !event.shiftKey) {
                event.preventDefault();
                sendMessage();
            }
        }

        function renderChart(chartData, title) {
            const wrapper = document.createElement('div');
            wrapper.classList.add('chart-container-wrapper');
            const chartContainer = document.createElement('div');
            chartContainer.classList.add('chart-container');
            const canvas = document.createElement('canvas');
            chartContainer.appendChild(canvas);
            wrapper.appendChild(chartContainer);
            chatMessages.appendChild(wrapper);

            new Chart(canvas, {
                type: 'line',
                data: chartData,
                options: {
                    responsive: true,
                    plugins: { legend: { position: 'top' }, title: { display: true, text: title } },
                    scales: {
                        y: {
                            beginAtZero: false,
                            ticks: {
                                callback: function(value) {
                                    if (Math.abs(value) >= 1e9) return (value / 1e9).toFixed(2) + 'B';
                                    if (Math.abs(value) >= 1e6) return (value / 1e6).toFixed(2) + 'M';
                                    if (Math.abs(value) >= 1e3) return (value / 1e3).toFixed(2) + 'K';
                                    return value;
                                }
                            }
                        }
                    }
                }
            });
            chatMessages.scrollTop = chatMessages.scrollHeight;
        }

        function addMessage(content, sender) {
            const messageElement = document.createElement('div');
            messageElement.classList.add('message', `${sender}-message`);
            messageElement.innerText = content;
            chatMessages.appendChild(messageElement);
            chatMessages.scrollTop = chatMessages.scrollHeight;
        }

        function showTypingIndicator() {
            const indicator = document.createElement('div');
            indicator.id = 'typing-indicator';
            indicator.classList.add('typing-indicator');
            indicator.innerHTML = '<span></span><span></span><span></span>';
            chatMessages.appendChild(indicator);
            chatMessages.scrollTop = chatMessages.scrollHeight;
        }

        function removeTypingIndicator() {
            const indicator = document.getElementById('typing-indicator');
            if (indicator) indicator.remove();
        }

        async function sendMessage() {
            const question = userInput.value.trim();
            if (!question) return;

            if (welcomeScreen && welcomeScreen.style.display !== 'none') {
                welcomeScreen.style.display = 'none';
            }

            addMessage(question, 'user');
            userInput.value = '';
            userInput.style.height = 'auto'; // 发送后重置高度
            showTypingIndicator();

            try {
                const res = await fetch('/ask', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ question: question })
                });
                const data = await res.json();
                
                removeTypingIndicator();

                if (data.error) {
                    addMessage(`Error: ${data.details || data.error}`, 'bot');
                } else if (data.type === 'chart_with_text') {
                    if (data.content) { addMessage(data.content, 'bot'); }
                    renderChart(data.data, data.title);
                } else if (data.type === 'chart') {
                    renderChart(data.data, data.title);
                } else if (data.type === 'text' && data.content) {
                    addMessage(data.content, 'bot');
                }

            } catch (error) {
                removeTypingIndicator();
                addMessage('An unexpected error occurred: ' + error, 'bot');
            }
        }
    </script>
</body>
</html>
"""

# Python 后端部分完全不变
app = Flask(__name__)

@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

@app.route('/ask', methods=['POST'])
def ask():
    if not all([DATABRICKS_HOST, GENIE_SPACE_ID, DATABRICKS_TOKEN]):
        return jsonify({"error": "Server is not configured. Missing environment variables."}), 500
    
    user_question = request.json.get('question')
    if not user_question:
        return jsonify({'error': 'Question cannot be empty'}), 400

    try:
        headers = {'Authorization': f'Bearer {DATABRICKS_TOKEN}', 'Content-Type': 'application/json'}
        ssl_verify_path = certifi.where()

        start_conv_url = f"{DATABRICKS_HOST}/api/2.0/genie/spaces/{GENIE_SPACE_ID}/start-conversation"
        start_payload = {'content': user_question}
        start_response = requests.post(start_conv_url, headers=headers, json=start_payload, verify=ssl_verify_path)
        start_response.raise_for_status()
        start_data = start_response.json()
        conversation_id = start_data['conversation']['id']
        message_id = start_data['message']['id']
        
        message_url = f"{DATABRICKS_HOST}/api/2.0/genie/spaces/{GENIE_SPACE_ID}/conversations/{conversation_id}/messages/{message_id}"
        status = ""
        poll_data = {}
        start_time = time.time()
        while status not in ['COMPLETED', 'FAILED', 'CANCELLED'] and time.time() - start_time < 300:
            time.sleep(3)
            poll_response = requests.get(message_url, headers=headers, verify=ssl_verify_path)
            poll_response.raise_for_status()
            poll_data = poll_response.json()
            status = poll_data.get('status')
        
        if status == 'COMPLETED':
            text_parts = []
            
            for attachment in poll_data.get('attachments', []):
                if 'text' in attachment:
                    content = attachment['text']
                    if isinstance(content, str):
                        text_parts.append(content)
                    elif isinstance(content, dict) and 'content' in content:
                        text_parts.append(content['content'])

                if 'query' in attachment and 'statement_id' in attachment['query']:
                    statement_id = attachment['query']['statement_id']
                    results_url = f"{DATABRICKS_HOST}/api/2.0/sql/statements/{statement_id}"
                    results_response = requests.get(results_url, headers=headers, verify=ssl_verify_path)
                    
                    if results_response.status_code == 200:
                        results_data = results_response.json()
                        manifest = results_data.get('manifest', {})
                        result = results_data.get('result', {})
                        
                        if manifest and result and len(manifest.get('schema', {}).get('columns', [])) == 2:
                            columns = manifest['schema']['columns']
                            data_array = result.get('data_array', [])
                            
                            col1_type = columns[0]['type_name'].lower()
                            col2_type = columns[1]['type_name'].lower()
                            is_chartable = (
                                ('date' in col1_type or 'string' in col1_type) and
                                ('long' in col2_type or 'int' in col2_type or 'double' in col2_type or 'float' in col2_type or 'decimal' in col2_type)
                            )

                            if is_chartable and data_array:
                                labels = [row[0] for row in data_array]
                                try:
                                    data_points = [float(row[1]) for row in data_array]
                                except (ValueError, TypeError):
                                    continue 

                                chart_data = {
                                    'labels': labels,
                                    'datasets': [{
                                        'label': columns[1]['name'].replace('_', ' ').title(),
                                        'data': data_points,
                                        'fill': False,
                                        'borderColor': '#6366f1',
                                        'tension': 0.1
                                    }]
                                }
                                
                                if text_parts:
                                    return jsonify({
                                        'type': 'chart_with_text',
                                        'title': f"Trend of {chart_data['datasets'][0]['label']}",
                                        'data': chart_data,
                                        'content': "\n\n".join(text_parts)
                                    })
                                else:
                                    return jsonify({
                                        'type': 'chart',
                                        'title': f"Trend of {chart_data['datasets'][0]['label']}",
                                        'data': chart_data
                                    })

                        if result.get('data_array'):
                            cols = [col['name'] for col in manifest['schema']['columns']]
                            rows = [" | ".join(map(str, row)) for row in result['data_array']]
                            table_text = " | ".join(cols) + "\n" + " | ".join(["---"] * len(cols)) + "\n" + "\n".join(rows)
                            text_parts.append(table_text)
            
            if text_parts:
                return jsonify({'type': 'text', 'content': "\n\n".join(text_parts)})
            else:
                return jsonify({'type': 'text', 'content': "I've processed your request, but couldn't find a specific answer or data."})
        else:
            return jsonify({'error': f'Failed to get answer. Final status: {status}', 'details': poll_data.get('error')}), 500

    except Exception as e:
        error_details = traceback.format_exc()
        return jsonify({'error': f'An unexpected server error occurred: {str(e)}', 'details': error_details}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)
