from app.performance_bundle import PERFORMANCE_METRICS


def normalize_intent(intent: dict, user_message: str) -> dict:
    msg = user_message.lower()

    # -------------------------
    # Initialize
    # -------------------------
    intent.setdefault("filters", {})
    intent.setdefault("metrics", [])
    intent.setdefault("dimensions", [])

    filters = intent["filters"]
    metrics = intent["metrics"]
    dimensions = intent["dimensions"]

    # -------------------------
    # Business Phrase → Filters
    # -------------------------
    if "winback" in msg:
        filters["campaign"] = "Winback"

    if "home services" in msg:
        filters["campaign"] = "Home Services"

    # -------------------------
    # Performance Bundle
    # -------------------------
    if "performance" in metrics:
        # Remove fake metric
        metrics = [m for m in metrics if m != "performance"]

        # Add KPI bundle
        for m in PERFORMANCE_METRICS:
            if m not in metrics:
                metrics.append(m)

    # -------------------------
    # Total Cost Alias
    # -------------------------
    # Normalize "total cost" -> total_spend handled by semantic layer
    # (No change needed here, semantic_layer handles it)

    # -------------------------
    # Remove Fake Dimensions
    # -------------------------
    cleaned_dimensions = []
    for d in dimensions:
        if d in ["winback campaigns", "winback", "home services campaigns"]:
            # These are filters, NOT dimensions
            continue
        cleaned_dimensions.append(d)

    intent["dimensions"] = cleaned_dimensions
    intent["metrics"] = metrics
    intent["filters"] = filters

    return intent
