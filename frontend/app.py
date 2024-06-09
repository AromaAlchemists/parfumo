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
opt_occasion = [
    "여가 (Leisure)",
    "일상 (Daily)",
    "외출 (Night out)",
    "업무 (Business)",
    "운동 (Sport)",
    "저녁 모임 (Evening)",
]


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

if "selected_values" not in st.session_state:
    st.session_state.selected_values = {
        "season": "",
        "audience": "",
        "occasion": "",
        "accord": [],
        "cnt": 0,
    }


def get_popular_prefset(season, audience):
    prefset = {
        ("봄", "젊은 (Youthful)"): ("여가 (Leisure)", ["Fresh", "Floral"]),
        ("봄", "성숙한 (Mature)"): ("업무 (Business)", ["Green", "Citrus"]),
        ("봄", "여성스러운 (Feminine)"): ("일상 (Daily)", ["Floral", "Fruity"]),
        ("봄", "남성스러운 (Masculine)"): ("외출 (Night out)", ["Woody", "Spicy"]),
        ("여름", "젊은 (Youthful)"): ("운동 (Sport)", ["Aquatic", "Citrus"]),
        ("여름", "성숙한 (Mature)"): ("일상 (Daily)", ["Fresh", "Green"]),
        ("여름", "여성스러운 (Feminine)"): ("외출 (Night out)", ["Fruity", "Floral"]),
        ("여름", "남성스러운 (Masculine)"): ("운동 (Sport)", ["Fresh", "Woody"]),
        ("가을", "젊은 (Youthful)"): ("여가 (Leisure)", ["Woody", "Spicy"]),
        ("가을", "성숙한 (Mature)"): ("업무 (Business)", ["Oriental", "Leathery"]),
        ("가을", "여성스러운 (Feminine)"): (
            "저녁 모임 (Evening)",
            ["Powdery", "Floral"],
        ),
        ("가을", "남성스러운 (Masculine)"): ("외출 (Night out)", ["Smoky", "Earthy"]),
        ("겨울", "젊은 (Youthful)"): ("여가 (Leisure)", ["Sweet", "Gourmand"]),
        ("겨울", "성숙한 (Mature)"): ("업무 (Business)", ["Smoky", "Resinous"]),
        ("겨울", "여성스러운 (Feminine)"): (
            "저녁 모임 (Evening)",
            ["Creamy", "Oriental"],
        ),
        ("겨울", "남성스러운 (Masculine)"): ("운동 (Sport)", ["Leathery", "Woody"]),
    }
    return prefset.get((season, audience), ("", []))


def update_selected_values(category, option):
    if category == 0:
        if st.session_state.selected_values["season"] == option:
            st.session_state.selected_values["season"] = ""
            st.session_state.selected_values["cnt"] -= 1
        else:
            if not st.session_state.selected_values["season"]:
                st.session_state.selected_values["cnt"] += 1
            st.session_state.selected_values["season"] = option

    elif category == 1:
        if st.session_state.selected_values["audience"] == option:
            st.session_state.selected_values["audience"] = ""
            st.session_state.selected_values["cnt"] -= 1
        else:
            if not st.session_state.selected_values["audience"]:
                st.session_state.selected_values["cnt"] += 1
            st.session_state.selected_values["audience"] = option

            if st.session_state.selected_values["season"]:
                occasion, accords = get_popular_prefset(
                    st.session_state.selected_values["season"],
                    st.session_state.selected_values["audience"],
                )

                if not st.session_state.selected_values["occasion"]:
                    st.session_state.selected_values["cnt"] += 1
                st.session_state.selected_values["occasion"] = occasion

                st.session_state.selected_values["cnt"] -= len(
                    st.session_state.selected_values["accord"]
                )
                st.session_state.selected_values["accord"] = accords
                st.session_state.selected_values["cnt"] += 2

    elif category == 2:
        if st.session_state.selected_values["occasion"] == option:
            st.session_state.selected_values["occasion"] = ""
            st.session_state.selected_values["cnt"] -= 1
        else:
            if not st.session_state.selected_values["occasion"]:
                st.session_state.selected_values["cnt"] += 1
            st.session_state.selected_values["occasion"] = option

    else:
        if option in st.session_state.selected_values["accord"]:
            st.session_state.selected_values["accord"].remove(option)
            st.session_state.selected_values["cnt"] -= 1


def selec_to_quick():
    st.session_state.quick_accord = [False] * len(opt_accord)
    st.session_state.quick_season = [False] * len(opt_season)
    st.session_state.quick_audience = [False] * len(opt_audience)
    st.session_state.quick_occasion = [False] * len(opt_occasion)
    st.session_state.quick_text = ""

    if st.session_state.selected_values["season"]:
        idx = opt_season.index(st.session_state.selected_values["season"])
        st.session_state.quick_season[idx] = True

    if st.session_state.selected_values["audience"]:
        idx = opt_audience.index(st.session_state.selected_values["audience"])
        st.session_state.quick_audience[idx] = True

    if st.session_state.selected_values["occasion"]:
        idx = opt_occasion.index(st.session_state.selected_values["occasion"])
        st.session_state.quick_occasion[idx] = True

    for accord in st.session_state.selected_values["accord"]:
        if accord in opt_accord:
            idx = opt_accord.index(accord)
            st.session_state.quick_accord[idx] = True

    st.session_state.quick_text = "recommend perfume"


def check_input():
    v_season = any(st.session_state.quick_season)
    v_audience = any(st.session_state.quick_audience)
    v_occasion = any(st.session_state.quick_occasion)
    v_text = st.session_state.quick_text != ""

    return v_season or v_audience or v_occasion or v_text


def get_quick_recommendation():
    # packaging user input
    input_quick = {
        "accord": [
            opt_accord[i]
            for i in range(len(opt_accord))
            if st.session_state.quick_accord[i]
        ],
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
        st.session_state.flag = 2
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
        st.session_state.flag = 2
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


# @st.experimental_fragment
def make_page():
    with st.container(height=400, border=False):
        with st.spinner("가장 적합한 향수를 찾고 있어요..."):
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

    col1, col2, col3, col4, col5 = st.columns([0.15, 0.15, 0.4, 0.15, 0.15])
    col1.button(
        "이전",
        on_click=prev_page,
        disabled=(st.session_state.page <= 0),
        use_container_width=True,
    )
    col2.button(
        "다음",
        on_click=next_page,
        disabled=(st.session_state.page >= 5),
        use_container_width=True,
    )
    col4.button(
        "간편추천",
        use_container_width=True,
        on_click=lambda: st.session_state.update(flag=0),
    )
    with col5:
        if st.button(
            "추천받기",
            type="primary",
            disabled=(st.session_state.page <= 1),
            use_container_width=True,
        ):
            if check_input():
                get_quick_recommendation()
            else:
                warning()


st.title("✨ Aroma Alchemist ✨")


# flag==0: 간편추천 화면
if st.session_state.flag == 0:
    st.markdown(
        """
        ##### 향수 추천 서비스 Aroma Alchemist에 오신 것을 환영합니다!
        """
    )
    st.text("")

    with st.container(height=150, border=True):
        if st.session_state.selected_values["cnt"]:
            c0, c1, c2, c3, c4 = st.columns(5)
            if st.session_state.selected_values["season"]:
                c0.button(
                    st.session_state.selected_values["season"],
                    use_container_width=True,
                    on_click=update_selected_values,
                    args=(
                        0,
                        st.session_state.selected_values["season"],
                    ),
                )
            if st.session_state.selected_values["audience"]:
                c1.button(
                    st.session_state.selected_values["audience"],
                    use_container_width=True,
                    on_click=update_selected_values,
                    args=(
                        1,
                        st.session_state.selected_values["audience"],
                    ),
                )
            if st.session_state.selected_values["occasion"]:
                c2.button(
                    st.session_state.selected_values["occasion"],
                    use_container_width=True,
                    on_click=update_selected_values,
                    args=(
                        2,
                        st.session_state.selected_values["occasion"],
                    ),
                )
            if len(st.session_state.selected_values["accord"]) >= 1:
                c3.button(
                    st.session_state.selected_values["accord"][0],
                    use_container_width=True,
                    on_click=update_selected_values,
                    args=(
                        3,
                        st.session_state.selected_values["accord"][0],
                    ),
                )
            if len(st.session_state.selected_values["accord"]) >= 2:
                c4.button(
                    st.session_state.selected_values["accord"][1],
                    use_container_width=True,
                    on_click=update_selected_values,
                    args=(
                        3,
                        st.session_state.selected_values["accord"][1],
                    ),
                )

        else:
            st.markdown("원하는 향수 타입을 선택해주세요.")
            st.markdown("계절과 느낌 선택에 따라 추천 조합을 제시해드립니다.")
            st.markdown("더 자세한 선택을 원하시면 [상세선택]을 이용해주세요.")

    c0, c1, c2 = st.columns([0.7, 0.15, 0.15])
    c1.button(
        "상세선택",
        use_container_width=True,
        on_click=lambda: st.session_state.update(flag=1),
    )
    if c2.button("추천받기", use_container_width=True, type="primary"):
        selec_to_quick()

        if check_input():
            get_quick_recommendation()
        else:
            warning()

    with st.sidebar:
        if not st.session_state.selected_values["season"]:
            st.markdown("**계절**")
            with st.container(border=False):
                cols = st.columns([0.25, 0.25, 0.25, 0.25])
                for i, col in enumerate(cols):
                    col.button(
                        opt_season[i],
                        key="season_" + str(i),
                        use_container_width=True,
                        on_click=update_selected_values,
                        args=(
                            0,
                            opt_season[i],
                        ),
                    )

        elif not st.session_state.selected_values["audience"]:
            st.markdown("**느낌**")
            with st.container(border=False):
                c1, c2 = st.columns([0.5, 0.5])
                c1.button(
                    opt_audience[0],
                    key="audience_0",
                    use_container_width=True,
                    on_click=update_selected_values,
                    args=(
                        1,
                        opt_audience[0],
                    ),
                )
                c2.button(
                    opt_audience[1],
                    key="audience_1",
                    use_container_width=True,
                    on_click=update_selected_values,
                    args=(
                        1,
                        opt_audience[1],
                    ),
                )
                c1.button(
                    opt_audience[2],
                    key="audience_2",
                    use_container_width=True,
                    on_click=update_selected_values,
                    args=(
                        1,
                        opt_audience[2],
                    ),
                )
                c2.button(
                    opt_audience[3],
                    key="audience_3",
                    use_container_width=True,
                    on_click=update_selected_values,
                    args=(
                        1,
                        opt_audience[3],
                    ),
                )

        else:
            st.markdown("**활동**")
            with st.container(border=False):
                c1, c2 = st.columns([0.5, 0.5])
                c1.button(
                    opt_occasion[0],
                    use_container_width=True,
                    key="occasion_0",
                    on_click=update_selected_values,
                    args=(
                        2,
                        opt_occasion[0],
                    ),
                )
                c2.button(
                    opt_occasion[1],
                    use_container_width=True,
                    key="occasion_1",
                    on_click=update_selected_values,
                    args=(
                        2,
                        opt_occasion[1],
                    ),
                )
                c1.button(
                    opt_occasion[2],
                    use_container_width=True,
                    key="occasion_2",
                    on_click=update_selected_values,
                    args=(
                        2,
                        opt_occasion[2],
                    ),
                )
                c2.button(
                    opt_occasion[3],
                    use_container_width=True,
                    key="occasion_3",
                    on_click=update_selected_values,
                    args=(
                        2,
                        opt_occasion[3],
                    ),
                )
                c1.button(
                    opt_occasion[4],
                    use_container_width=True,
                    key="occasion_4",
                    on_click=update_selected_values,
                    args=(
                        2,
                        opt_occasion[4],
                    ),
                )
                c2.button(
                    opt_occasion[5],
                    use_container_width=True,
                    key="occasion_5",
                    on_click=update_selected_values,
                    args=(
                        2,
                        opt_occasion[5],
                    ),
                )


# flag==1: 상세 선택 및 채팅 화면
elif st.session_state.flag == 1:
    st.markdown(
        """
        ##### 향수 추천 서비스 Aroma Alchemist에 오신 것을 환영합니다!
        당신이 찾는 향수에 대해 알려주세요.
        """
    )
    st.text("")

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


# flag==2: 결과 화면
else:
    st.markdown(
        """
        ##### 추천 결과:
        아래는 당신의 취향과 선호도를 반영한 맞춤형 향수 추천 목록입니다.
        """
    )
    for perfume in st.session_state.recommendation:
        name = perfume["perfume"]
        year = perfume["released_year"]
        brand = perfume["brand"]
        description = perfume["description"]
        image_url = perfume["img_url"]
        rating = perfume["rating"]
        link = perfume["url"]
        with st.container(height=None, border=True):
            col1, col2 = st.columns([0.7, 0.3])
            with col1:
                st.markdown(f"🧴 **{name}** ({year}) from {brand}")
                st.markdown(f"rating: *{rating}*")
                st.markdown(f"{description}...")
                st.link_button("Link", link, help="more info at www.parfumo.com")
            with col2:
                st.image(image_url, width=150)
    col1, col2 = st.columns([0.7, 0.3])
    with col2:
        st.button(
            "선택으로 돌아가기",
            on_click=lambda: st.session_state.update(flag=0),
        )
