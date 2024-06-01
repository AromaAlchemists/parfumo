from models import Preferences, Chat


def scoring():
    pass


def rag():
    pass


def do_scoring_and_rag(prefinput: Preferences):
    scoring()  # -> 1차 결과
    # DB - 1차 결과에 해당하는 데이터 가져오기...
    rag()

    # === sample result ===
    recommendations = [
        "Gentleman Givenchy Réserve Privée",
        "Apex",
        "Terre d'Hermès Eau Givrée",
    ]

    return {"recommendations": recommendations}


def do_rag(chatinput: Chat):
    rag()

    # === sample result ===
    recommendations = [
        "Gentleman Givenchy Réserve Privée",
        "Apex",
        "Terre d'Hermès Eau Givrée",
    ]

    return {"recommendations": recommendations}
