import os
from pathlib import Path
import panel as pn
from lake.pages import load_page

# Ensure Panel extensions are loaded; sizing_mode will make things responsive
pn.extension('ipywidgets', sizing_mode='stretch_both')


# Use a polished dark template (Vuetify). Set dark=True for a true dark theme.
# template = pn.template.DarkTheme()

# Directory containing page modules
PAGES_DIR = Path('.') / 'lake' / 'pages'
IGNORE_FILES = {'__pycache__', '__init__.py'}
def discover_modules():
    """Return list of available page module names (without .py)."""
    if not PAGES_DIR.exists():
        return []
    return sorted(
        fname.stem
        for fname in PAGES_DIR.iterdir()
        if fname.is_file() and fname.suffix == '.py' and fname.name not in IGNORE_FILES
    )

def render_dashboard(module_name):
    """Render a dashboard page by name. Expects instance.deploy() -> Matplotlib Figure."""
    try:

        config_path = os.getenv("LAKE_CONFIG","./resources/config.local.yml")
        print(f"LOADING MODULE {module_name} {config_path}")

        instance = load_page(module_name, config_path)
        fig = instance.deploy()
        pane = pn.pane.Matplotlib(fig, tight=True, sizing_mode='stretch_both')
        card = pn.Card(pane, title=f"{module_name}", sizing_mode='stretch_both')
        return card
    except Exception as exc:
        # Return a friendly error pane if a page fails to render
        return pn.Card(
            pn.pane.Markdown(f"### Error loading `{module_name}`\n\n```\n{exc}\n```"),
            title=f"{module_name} — Error",
            sizing_mode='stretch_both'
        )

# Discover and pre-render dashboards (keeps original behavior of pre-loading)
available_modules = discover_modules()
dashboards = {name: render_dashboard(name) for name in available_modules}

# Sidebar widgets
sidebar_select = pn.widgets.Select(
    name='Select Dashboard',
    options=available_modules,
    value=available_modules[0] if available_modules else None,
    sizing_mode='stretch_width'
)

# Add a small header for branding
sidebar_header = pn.Column(
    pn.pane.Markdown("## Decoded\n### ducklake-sdk"),
    pn.pane.Markdown("Free Space to add widgets on demand"),
    sizing_mode='stretch_width'
)

# Refresh button to re-discover and re-render modules (useful for dev)
refresh_button = pn.widgets.Button(name='Refresh Pages', button_type='primary', sizing_mode='stretch_width')

def refresh_pages(event=None):
    global available_modules, dashboards
    available_modules = discover_modules()
    sidebar_select.options = available_modules
    dashboards = {name: render_dashboard(name) for name in available_modules}
    if sidebar_select.value and sidebar_select.value in dashboards:
        view_dashboard(type('Event', (), {'new': sidebar_select.value})())
    elif available_modules:
        sidebar_select.value = available_modules[0]

refresh_button.on_click(refresh_pages)

# Assemble sidebar into a card
sidebar_card = pn.Card(
    pn.Column(sidebar_header, sidebar_select, refresh_button),
    title="Navigation",
    sizing_mode='stretch_width',
    collapsible=False
)

# Main dashboard panel with initial content
initial = dashboards.get(sidebar_select.value) if sidebar_select.value else pn.pane.Markdown("No dashboards found.")
dashboard_panel = pn.Column(initial, sizing_mode='stretch_both')

# Status footer
status = pn.Row(
    pn.pane.Markdown("### Status"),
    pn.pane.Markdown("- Development mode"),
    sizing_mode='stretch_width'
)

# Watcher to update the main panel
def view_dashboard(event):
    selected = event.new
    if selected in dashboards:
        dashboard_panel[:] = [dashboards[selected]]
    else:
        dashboard_panel[:] = [pn.pane.Markdown("Dashboard not found.", style={'color': '#FFCDD2'})]

sidebar_select.param.watch(view_dashboard, 'value')


template = pn.template.MaterialTemplate(
    site="Panel",
    title="Decoded Ducklake Demo (DDD)",
    sidebar=[sidebar_card],
    main=[dashboard_panel]
)

def serve():
    # Expose the template as servable and run the server
    template.servable(title='BI as Code panel')
    pn.serve(template, title='BI as Code panel',address='0.0.0.0',port=5480, show=False)

if __name__ == '__main__':
    serve()
# elif __name__ == "lake.render":
    
#     import .render