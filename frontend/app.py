import streamlit as st


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


# page configuration (title on the tab)
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
        st.form_submit_button("Get Recommendations")


# val_audience
# val_season
# val_occasion
# val_text
