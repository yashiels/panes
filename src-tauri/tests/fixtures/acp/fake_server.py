import json
import os
import sys

scenario = sys.argv[1]
log_path = sys.argv[2]
session = 'fake-durable-session'
prompt_count = 0


def receive():
    line = sys.stdin.buffer.readline()
    if not line:
        sys.exit(0)
    value = json.loads(line)
    with open(log_path, 'a') as log:
        log.write(json.dumps(value) + '\n')
    return value


def send(value, fragmented=False):
    raw = (json.dumps(value) + '\n').encode()
    if fragmented:
        for index in range(0, len(raw), 7):
            sys.stdout.buffer.write(raw[index:index + 7])
            sys.stdout.buffer.flush()
    else:
        sys.stdout.buffer.write(raw)
        sys.stdout.buffer.flush()


def result(request_id, value):
    send({'jsonrpc': '2.0', 'id': request_id, 'result': value})


def update(value):
    send({'jsonrpc': '2.0', 'method': 'session/update', 'params': {'sessionId': session, 'update': value}})


def permission(request_id):
    send({'jsonrpc': '2.0', 'id': request_id, 'method': 'session/request_permission', 'params': {
        'sessionId': session,
        'toolCall': {'toolCallId': 'tool-1', 'title': 'Read fixture', 'kind': 'read'},
        'options': [{'optionId': 'original/allow-once', 'name': 'Allow', 'kind': 'allow_once'},
                    {'optionId': 'original/deny', 'name': 'Deny', 'kind': 'reject_once'}]}})


while True:
    request = receive()
    method = request.get('method')
    request_id = request.get('id')
    if method == 'initialize':
        if scenario == 'slow_init':
            import time
            time.sleep(10)
        if scenario == 'timeout_init':
            continue
        if scenario == 'malformed_init':
            sys.stdout.write('not json\n'); sys.stdout.flush()
            continue
        if scenario == 'death_init':
            sys.exit(12)
        send({'jsonrpc': '2.0', 'id': request_id, 'method': 'fs/read_text_file',
              'params': {'sessionId': session, 'path': '/unsupported'}})
        answer = receive()
        assert answer['id'] == request_id and answer['error']['code'] == -32601
        send({'jsonrpc': '2.0', 'id': request_id, 'result': {
            'protocolVersion': 99 if scenario == 'version' else 1,
            'agentCapabilities': {'loadSession': scenario != 'no_load'},
            'agentInfo': {'name': 'fake', 'version': '1'}}}, fragmented=True)
    elif method in ('session/new', 'session/load'):
        if method == 'session/load':
            session = request['params']['sessionId']
            update({'sessionUpdate': 'agent_message_chunk', 'content': {'type': 'text', 'text': 'history'}})
            result(request_id, {})
        else:
            response = {'sessionId': session}
            if scenario == 'hermes':
                from pathlib import Path
                frames = [json.loads(line) for line in Path(__file__).with_name('hermes-text.jsonl').read_text().splitlines()]
                response['models'] = next(frame['result']['models'] for frame in frames if 'models' in frame.get('result', {}))
            result(request_id, response)
    elif method == 'session/set_config_option' and scenario == 'agy':
        assert request['params']['configId'] == 'model'
        from pathlib import Path
        frames = [json.loads(line) for line in Path(__file__).with_name('agy-prompt.jsonl').read_text().splitlines()]
        options = next(frame['params']['update'] for frame in frames if any(option['id'] == 'model' for option in frame.get('params', {}).get('update', {}).get('configOptions', [])))
        result(request_id, {'configOptions': options['configOptions']})
    elif method == 'session/set_model':
        result(request_id, {})
    elif method == 'session/prompt':
        prompt_count += 1
        if scenario == 'agy':
            from pathlib import Path
            frames = [json.loads(line) for line in Path(__file__).with_name('agy-prompt.jsonl').read_text().splitlines()]
            for frame in frames:
                if frame.get('method') == 'session/update':
                    frame['params']['sessionId'] = session
                    send(frame)
                elif 'result' in frame:
                    result(request_id, frame['result'])
            continue
        if scenario == 'death':
            sys.exit(17)
        if scenario == 'malformed':
            sys.stdout.write('{not json}\n'); sys.stdout.flush()
            continue
        if scenario == 'oversized':
            sys.stdout.write('x' * (1024 * 1024 + 1)); sys.stdout.flush()
            continue
        if scenario == 'partial':
            sys.stdout.write('{"jsonrpc":'); sys.stdout.flush()
            sys.exit(18)
        if scenario == 'bad_update':
            update({'sessionUpdate': 'agent_message_chunk', 'content': {'type': 'text'}})
            continue
        if scenario == 'wrong_session':
            send({'jsonrpc': '2.0', 'method': 'session/update', 'params': {'sessionId': 'wrong', 'update': {'sessionUpdate': 'agent_message_chunk', 'content': {'type': 'text', 'text': 'wrong'}}}})
            continue
        if scenario == 'rpc_error':
            send({'jsonrpc': '2.0', 'id': request_id, 'error': {'code': -32603, 'message': 'fixture error'}})
            continue
        update({'sessionUpdate': 'agent_thought_chunk', 'content': {'type': 'text', 'text': 'thinking'}})
        if scenario in ('wait', 'timeout'):
            permission(7)
            continue
        if scenario == 'duplicate_permission':
            permission(7); permission(7)
            continue
        update({'sessionUpdate': 'future_optional_update', 'value': True})
        update({'sessionUpdate': 'tool_call', 'toolCallId': 'tool-1', 'title': 'Read fixture', 'kind': 'read', 'status': 'in_progress'})
        for text in ['a', 'ab', 'ab']:
            update({'sessionUpdate': 'tool_call_update', 'toolCallId': 'tool-1', 'content': [{'type': 'content', 'content': {'type': 'text', 'text': text}}]})
        permission(7)
        answer = receive()
        assert answer['id'] == 7
        assert answer['result']['outcome'] == {'outcome': 'selected', 'optionId': 'original/allow-once'}
        for _ in range(2):
            update({'sessionUpdate': 'tool_call_update', 'toolCallId': 'tool-1', 'status': 'completed', 'content': [{'type': 'content', 'content': {'type': 'text', 'text': 'abc'}}]})
        update({'sessionUpdate': 'agent_message_chunk', 'content': {'type': 'text', 'text': 'hello '}})
        update({'sessionUpdate': 'agent_message_chunk', 'content': {'type': 'text', 'text': 'world'}})
        result(request_id, {'stopReason': 'end_turn', 'usage': {'totalTokens': 15, 'inputTokens': 10, 'outputTokens': 5, 'thoughtTokens': 2}})
        if scenario == 'duplicate_completion':
            result(request_id, {'stopReason': 'end_turn'})
    elif method == 'session/cancel':
        if scenario == 'wait':
            result('client-3', {'stopReason': 'cancelled'})
    elif method is None:
        assert request.get('result', {}).get('outcome', {}).get('outcome') == 'cancelled'
    else:
        send({'jsonrpc': '2.0', 'id': request_id, 'error': {'code': -32601, 'message': 'unsupported'}})
