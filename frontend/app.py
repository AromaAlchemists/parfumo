import streamlit as st
import requests


SERVER_URL = "http://127.0.0.1:8000"


st.set_page_config(
    page_title="Aroma Alchemist - AA",
    page_icon="🧴",
)


# option lists for each category
opt_accord = [
    "Sweet",
    "Spicy",
    "Oriental",
    "Powdery",
    "Woody",
    "Gourmand",
    "Fresh",
    "Synthetic",
    "Green",
    "Aquatic",
    "Citrus",
    "Creamy",
    "Fruity",
    "Floral",
    "Smoky",
    "Resinous",
    "Leathery",
    "Earthy",
    "Chypre",
    "Animal",
    "Fougère",
]
opt_season = ["봄", "여름", "가을", "겨울"]
opt_audience = [
    "젊은 (Youthful)",
    "성숙한 (Mature)",
    "여성스러운 (Feminine)",
    "남성스러운 (Masculine)",
]
opt_occasion = ["Leisure", "Daily", "Night out", "Business", "Sport", "Evening"]
# category = ["quick_accord", "quick_season", "quick_audience", "quick_occasion", ]


# value init. in session_state
if "quick_accord" not in st.session_state:
    st.session_state.quick_accord = [False] * len(opt_accord)
if "quick_season" not in st.session_state:
    st.session_state.quick_season = [False] * len(opt_season)
if "quick_audience" not in st.session_state:
    st.session_state.quick_audience = [False] * len(opt_audience)
if "quick_occasion" not in st.session_state:
    st.session_state.quick_occasion = [False] * len(opt_occasion)
if "quick_text" not in st.session_state:
    st.session_state.quick_text = ""

if "chat_text" not in st.session_state:
    st.session_state.chat_text = ""

# result from RecSys
if "recommendation" not in st.session_state:
    st.session_state.recommendation = []
# control flag for displaying result
if "flag" not in st.session_state:
    st.session_state.flag = False

if "page" not in st.session_state:
    st.session_state.page = 0


def check_input():
    v_season = any(st.session_state.quick_season)
    v_audience = any(st.session_state.quick_audience)
    v_occasion = any(st.session_state.quick_occasion)
    v_text = st.session_state.quick_text != ""

    return v_season or v_audience or v_occasion or v_text


def get_quick_recommendation():
    # packaging user input
    input_quick = {
        #        "accord": [
        #            opt_accord[i]
        #            for i in range(len(opt_accord))
        #            if st.session_state.quick_accord[i]
        #        ],
        "audience": st.session_state.quick_audience,
        "season": st.session_state.quick_season,
        "occasion": st.session_state.quick_occasion,
        "text": st.session_state.quick_text,
    }

    try:
        # send request & get response from server
        response = requests.post(SERVER_URL + "/quick-recommendation", json=input_quick)
        response.raise_for_status()
        st.session_state.recommendation = (
            response.json()
        )  # keep recommendation result in session_state.recommendations
        st.session_state.flag = True
        st.rerun()

    # error handling
    except requests.RequestException as e:
        st.error(f"Failed to get recommendations: {e}")


def get_chat_recommendation():
    input_chat = {
        "chat": st.session_state.chat_text,
    }
    try:
        # send request & get response from server
        response = requests.post(SERVER_URL + "/chat-recommendation", json=input_chat)
        response.raise_for_status()
        st.session_state.recommendation = (
            response.json()
        )  # keep recommendation result in session_state.recommendations
        st.session_state.flag = True
        st.rerun()

    # error handling
    except requests.RequestException as e:
        st.error(f"Failed to get recommendations: {e}")


def prev_page():
    if st.session_state.page > 0:
        st.session_state.page -= 1


def next_page():
    if st.session_state.page < 5:
        st.session_state.page += 1


@st.experimental_dialog("⚠️")
def warning():
    st.write("추천을 위해서 하나 이상의 항목을 선택해주세요!")


# UI - quick_recommendation
@st.experimental_fragment
def make_page():
    if st.session_state.page == 0:
        st.markdown("빠르고 쉽게 원하는 향수를 찾으실 수 있게 도와드립니다.")
        st.markdown("추천은 아래와 같이 5단계에 걸쳐 진행됩니다.")
        st.markdown("1. Accord - 선호하는 향 타입")
        st.markdown("2. Season - 사용할 계절")
        st.markdown("3. Audience - 향수의 느낌")
        st.markdown("4. Occasion - 사용할 자리")
        st.markdown("5. 추가 정보 입력")
        st.markdown(
            "각 단계에서 해당 사항이 없으면 선택 없이 넘어가실 수 있지만, 추천을 위해서는 2/3/4 단계 중 최소 한 가지 항목의 선택이 필요합니다."
        )

    elif st.session_state.page == 1:
        st.markdown("원하는 향을 선택해주세요.")
        k = int(len(opt_accord) / 3)
        col1, col2, col3 = st.columns(3)
        with col1:
            for i in range(k):
                st.session_state.quick_accord[i] = st.checkbox(
                    opt_accord[i], value=st.session_state.quick_accord[i]
                )
        with col2:
            for i in range(k, k * 2):
                st.session_state.quick_accord[i] = st.checkbox(
                    opt_accord[i], value=st.session_state.quick_accord[i]
                )
        with col3:
            for i in range(k * 2, len(opt_accord)):
                st.session_state.quick_accord[i] = st.checkbox(
                    opt_accord[i], value=st.session_state.quick_accord[i]
                )

    elif st.session_state.page == 2:
        st.markdown("사용하고 싶은 계절을 선택해주세요.")
        for i in range(len(opt_season)):
            st.session_state.quick_season[i] = st.checkbox(
                opt_season[i], value=st.session_state.quick_season[i]
            )

    elif st.session_state.page == 3:
        st.markdown("어떤 느낌을 원하시나요?")
        for i in range(len(opt_audience)):
            st.session_state.quick_audience[i] = st.checkbox(
                opt_audience[i], value=st.session_state.quick_audience[i]
            )

    elif st.session_state.page == 4:
        st.markdown("어느 자리에서 사용하고 싶나요?")
        for i in range(len(opt_occasion)):
            st.session_state.quick_occasion[i] = st.checkbox(
                opt_occasion[i], value=st.session_state.quick_occasion[i]
            )

    elif st.session_state.page == 5:
        st.markdown("추가로 원하시는 것을 자유롭게 입력해주세요.")
        st.session_state.quick_text = st.text_area(
            label="quick_text",
            value=st.session_state.quick_text,
            label_visibility="collapsed",
        )

    col1, col2, col3, col4 = st.columns([0.1, 0.1, 0.6, 0.2])
    with col1:
        st.button("이전", on_click=prev_page, disabled=(st.session_state.page <= 0))
    with col2:
        st.button("다음", on_click=next_page, disabled=(st.session_state.page >= 5))
    with col4:
        if st.button(
            "추천 받기", type="primary", disabled=(st.session_state.page <= 1)
        ):
            if check_input():
                get_quick_recommendation()
            else:
                warning()


# UI - intro
st.title("✨ Aroma Alchemist ✨")
st.markdown(
    """
#### Welcome!

I'm your personal perfume advisor.

You can tell me what type of perfume you're looking for, or simply select your preferences on the sidebar for a quick recommendation!
"""
)

if not st.session_state.flag:
    tab_quick, tab_chat = st.tabs(
        ["⚡ Quick Recommendation", "💬 Recommendation by Chat"]
    )

    with tab_quick:
        make_page()

    with tab_chat:
        st.markdown(
            "채팅으로 자유롭게 찾고자 하는 향수에 대해 얘기해주시면, 사용 후기를 바탕으로 가장 적합한 향수를 추천해드릴게요!"
        )
        st.session_state.chat_text = st.chat_input("여기에 입력해주세요")
        if st.session_state.chat_text:
            get_chat_recommendation()

else:
    st.subheader("Recommendation Result:")
    for perfume in st.session_state.recommendation:
        name = perfume["perfume_name"]
        year = perfume["year"]
        brand = perfume["brand"]
        image_url = perfume["image_url"]
        rating = perfume["rating"]
        link = perfume["link"]
        with st.container(height=None, border=True):
            col1, col2 = st.columns([0.7, 0.3])
            with col1:
                st.markdown(f"🧴 **{name}** ({year}) from {brand}")
                st.markdown(f"rating: *{rating}*")
                st.link_button("Link", link, help="more info at www.parfumo.com")
            with col2:
                st.image(image_url, width=150)

    col1, col2 = st.columns([0.7, 0.3])
    with col2:
        st.button(
            "선택으로 돌아가기", on_click=lambda: st.session_state.update(flag=False)
        )

# 이전 결과 보기 버튼 추가
