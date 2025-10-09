import panel as pn
import numpy as np
import matplotlib.pyplot as plt
import os
from lake.pages import load_page
ignore = ['__pycache__','__init__.py']
available_modules = [filename.replace('.py','') for filename in os.listdir('./lake/pages/') if filename not in ignore]


# Ensure the Panel extension is loaded
pn.extension('ipywidgets')

# Function to render a dashboard
def render_dashboard(module_name):
    """Render the dashboard with the given name."""
    print(f"LOADING MODULE {module_name}")
    instance = load_page(module_name,'resources/config.tmp.yml')
    plot = instance.deploy()
    dashboard = pn.Column(pn.pane.Matplotlib(plot, tight=True), width=600)
    return dashboard

# Sidebar with navigation
sidebar = pn.widgets.Select(name='Select Dashboard', options=available_modules)

# Creating initial dashboards
dashboards = {name: render_dashboard(name) for name in sidebar.options}

# Function to update the displayed dashboard
def view_dashboard(event):
    selected_dashboard = event.new
    dashboard_panel[:] = [dashboards[selected_dashboard]]

# Initial dashboard
dashboard_panel = pn.Column(dashboards[sidebar.value])
print("PANEL VALUES",dashboard_panel)
print("SIDEBAR",sidebar)
layout = pn.Row(sidebar, dashboard_panel,height=600, width_policy='max')

def serve():
    sidebar.param.watch(view_dashboard, 'value')

    # Display the layout
    main_layout = pn.Column(layout)
    main_layout.servable(title='Bi as Code panel')
    print(f"serving {main_layout}")
    pn.serve(main_layout)
    
# Run the app
if __name__ == '__main__':
    serve()