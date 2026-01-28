from app.gpt_client import choose_render_format

def build_render_spec(question: str, rows: list[dict]) -> dict:
    return choose_render_format(question, rows)
