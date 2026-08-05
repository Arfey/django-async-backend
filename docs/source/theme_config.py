colors = {
    "bg0": " #fbf1c7",
    "bg1": " #ebdbb2",
    "fg0": " #282828",
    "fg1": " #2e2e2e",
    "fg2": " #504945",
    "yellow": " #d79921",
    "green": " #98971a",
    "green2": " #79740e",
    "blue": " #458588",
    "blue2": " #076678",
}

html_theme = "furo"
html_theme_options = {
    "light_css_variables": {
        "color-brand-primary": colors["green2"],
        "color-brand-content": colors["blue2"],
    },
    "dark_css_variables": {
        "color-brand-primary": colors["green"],
        "color-brand-content": colors["blue"],
        "color-background-primary": colors["fg1"],
        "color-background-secondary": colors["fg0"],
        "color-foreground-primary": colors["bg0"],
        "color-foreground-secondary": colors["bg1"],
        "color-highlighted-background": colors["yellow"],
        "color-highlight-on-target": colors["fg2"],
    },
}

highlight_language = "python3"
pygments_style = "gruvbox-light"
pygments_dark_style = "gruvbox-dark"
