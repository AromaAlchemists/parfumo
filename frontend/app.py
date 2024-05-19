import streamlit as st
import requests


SERVER_URL = "http://127.0.0.1:8000/recommend"

# values that will be sent to the server
val_audience = [0, 0, 0, 0]
val_season = [0, 0, 0, 0]
val_occasion = [0, 0, 0, 0, 0, 0]
val_text = ""

# option lists for each category
opts_audience = ["Youthful", "Mature", "Feminine", "Masculine"]
opts_season = ["Spring", "Summer", "Fall", "Winter"]
opts_occasion = ["Leisure", "Daily", "Night out", "Business", "Sport", "Evening"]
optcnt_audience = len(opts_audience)
optcnt_season = len(opts_season)
optcnt_occasion = len(opts_occasion)

# initialize session state for recommendation result
if "recommendations" not in st.session_state:
    st.session_state.recommendations = []


# (callback function for form button) send user input data to server
def get_recommendation():
    data = {
        "audience": val_audience,
        "season": val_season,
        "occasion": val_occasion,
        "text": val_text,
    }

    try:
        response = requests.post(SERVER_URL, json=data)
        response.raise_for_status()
        recommendations = response.json()
        st.session_state.recommendations = recommendations

    # error handling
    except requests.RequestException as e:
        st.error(f"Failed to get recommendations: {e}")


# page configuration (browser tab title)
st.set_page_config(
    page_title="Aroma Alchemist - AA",
    page_icon="🧴",
)


# UI - intro
st.title("Aroma Alchemist")
st.markdown(
    """
#### Welcome!

I'm your personal perfume advisor.

You can tell me what type of perfume you're looking for, or simply select your preferences on the sidebar for a quick recommendation!
"""
)


# UI - sidebar / Quick Recommendation
with st.sidebar:
    st.header("Quick Recommendation", divider="rainbow")

    # toggle button for page switching (simple(default) <-> detailed)
    isdetailed = st.toggle("Fine-tune Options")

    # set form container to collect inputs
    with st.form("sidebar", border=False):
        # UI for simple choice
        if not isdetailed:
            st.subheader("Audience:")
            for i in range(optcnt_audience):
                val_audience[i] = st.checkbox(opts_audience[i])
            "\n"
            st.subheader("Season:")
            for i in range(optcnt_season):
                val_season[i] = st.checkbox(opts_season[i])
            "\n"
            st.subheader("Occasion:")
            for i in range(optcnt_occasion):
                val_occasion[i] = st.checkbox(opts_occasion[i])
            "\n"

        # UI for detailed choice
        else:
            st.subheader("Audience:")
            for i in range(optcnt_audience):
                val_audience[i] = st.slider(
                    opts_audience[i],
                    max_value=100,
                    step=10,
                    key=("slider_" + opts_audience[i]),
                )
            "\n"
            st.subheader("Season:")
            for i in range(optcnt_season):
                val_season[i] = st.slider(
                    opts_season[i],
                    max_value=100,
                    step=10,
                    key=("slider_" + opts_season[i]),
                )
            "\n"
            st.subheader("Occasion:")
            for i in range(optcnt_occasion):
                val_occasion[i] = st.slider(
                    opts_occasion[i],
                    max_value=100,
                    step=10,
                    key=("slider_" + opts_occasion[i]),
                )
            "\n"

        # UI for additional text input
        with st.expander("Additional Input:"):
            val_text = st.text_area(
                label="text_area",
                placeholder="Type for more personal preferences",
                label_visibility="collapsed",
            )

        # UI for submit button
        st.form_submit_button("Get Recommendations", on_click=get_recommendation)


# val_audience
# val_season
# val_occasion
# val_text
# 조건 입력 없이 버튼 클릭 case handle

# st.session_state.recommendations
# UI - display recommendation result
if st.session_state.recommendations:
    with st.container():
        st.header("Recommendation Result:")

        for perfume in st.session_state.recommendations:
            name = perfume["perfume_name"]
            year = perfume["year"]
            brand = perfume["brand"]
            image_url = perfume["image_url"]
            notes_top = perfume["notes_top"]
            notes_heart = perfume["notes_heart"]
            notes_base = perfume["notes_base"]
            rating = perfume["rating"]
            link = perfume["link"]

            with st.container(height=None, border=True):
                st.markdown(f"🧴 **{name}** ({year}) from {brand}")
                col1, col2 = st.columns(2)
                with col1:
                    st.image(image_url)
                with col2:
                    st.markdown(f"rating: *{rating}*")
                    st.link_button("Link", link, help="more info at www.parfumo.com")
