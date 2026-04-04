import math

import pandas as pd
import pydeck as pdk
from config import PLANE_ICON_URL

_TOOLTIP = {
    "html": """<div style='font-family:monospace;font-size:12px;background:#0d1825;
    border:1px solid #1a2e42;padding:8px 12px;border-radius:6px;color:#c8d8e8'>
    {tooltip}</div>""",
    "style": {"background": "transparent", "border": "none"},
}

_MAP_STYLE = "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json"
_DEFAULT_VIEW = pdk.ViewState(latitude=20, longitude=0, zoom=1.8, pitch=0, bearing=0)

_ICON_DATA = {
    "url": PLANE_ICON_URL,
    "width": 128,
    "height": 128,
    "anchorY": 64,
}


def build_seismic_layers(seismics: pd.DataFrame) -> list:
    if seismics.empty:
        return []

    layers = [
        pdk.Layer(
            "ScatterplotLayer",
            id="seismic-core",
            data=seismics,
            get_position=["lon", "lat"],
            get_radius="radius",
            get_fill_color="color",
            get_line_color=[255, 100, 50, 220],
            line_width_min_pixels=1,
            stroked=True,
            filled=True,
            pickable=True,
        )
    ]

    for i, (scale, alpha) in enumerate([(1.8, 55), (2.8, 28), (4.0, 10)], start=1):
        ring = seismics.copy()
        ring["_r"] = ring["radius"] * scale
        ring["_c"] = ring["color"].apply(lambda c: [c[0], c[1], c[2], alpha])
        layers.append(
            pdk.Layer(
                "ScatterplotLayer",
                id=f"seismic-ring-{i}",
                data=ring,
                get_position=["lon", "lat"],
                get_radius="_r",
                get_fill_color=[0, 0, 0, 0],
                get_line_color="_c",
                line_width_min_pixels=2 if i == 1 else 1,
                stroked=True,
                filled=False,
                pickable=False,
            )
        )

    return layers


def build_live_deck(
    flights: pd.DataFrame,
    weather: pd.DataFrame,
    seismics: pd.DataFrame,
    airports: pd.DataFrame,
    routes: pd.DataFrame,
    show_flights: bool,
    show_weather: bool,
    show_seismics: bool,
    show_airports: bool,
    show_density: bool,
    show_routes: bool,
    show_tracks: bool,
) -> pdk.Deck:
    layers: list = []

    if show_weather and not weather.empty:
        layers.append(
            pdk.Layer(
                "HeatmapLayer",
                data=weather,
                get_position=["lon", "lat"],
                get_weight="weight",
                radius_pixels=250,
                intensity=1.2,
                threshold=0.01,
                color_range=[
                    [0, 50, 150, 0],
                    [0, 100, 200, 40],
                    [100, 0, 200, 80],
                    [180, 0, 255, 120],
                ],
                pickable=False,
            )
        )
        layers.append(
            pdk.Layer(
                "HeatmapLayer",
                data=weather,
                get_position=["lon", "lat"],
                get_weight="weight",
                radius_pixels=250,
                intensity=1.2,
                threshold=0.01,
                color_range=[
                    [0, 50, 150, 0],
                    [0, 100, 200, 40],
                    [100, 0, 200, 80],
                    [180, 0, 255, 120],
                ],
                pickable=False,
            )
        )

        # --- Nueva: temperatura (heatmap azul→rojo) ---
        temp_data = weather.copy()
        # normaliza -30°C..+50°C → 0..1
        temp_data["temp_weight"] = (temp_data["temperature_c"].clip(-30, 50) + 30) / 80
        layers.append(
            pdk.Layer(
                "HeatmapLayer",
                id="weather-temp",
                data=temp_data,
                get_position=["lon", "lat"],
                get_weight="temp_weight",
                radius_pixels=300,
                intensity=0.8,
                threshold=0.01,
                opacity=0.35,  # suave para no tapar vuelos
                color_range=[
                    [0, 100, 255, 0],  # frío → azul
                    [0, 200, 200, 40],
                    [255, 200, 0, 80],
                    [255, 80, 0, 110],  # caliente → rojo
                ],
                pickable=False,
            )
        )

        # --- Nueva: visibilidad baja (heatmap gris-blanco) ---
        low_vis = weather[
            weather["vis_weight"] > 0.1
        ].copy()  # solo donde hay niebla/nube
        if not low_vis.empty:
            layers.append(
                pdk.Layer(
                    "HeatmapLayer",
                    id="weather-visibility",
                    data=low_vis,
                    get_position=["lon", "lat"],
                    get_weight="vis_weight",
                    radius_pixels=200,
                    intensity=1.0,
                    threshold=0.05,
                    opacity=0.4,
                    color_range=[
                        [200, 220, 255, 0],
                        [200, 220, 255, 60],
                        [220, 230, 255, 120],
                        [240, 245, 255, 180],  # blanco-azulado para niebla
                    ],
                    pickable=False,
                )
            )

        rain = weather[weather["precipitation_mm"] > 0.1].copy()
        if not rain.empty:
            rain["precip_radius"] = (
                rain["precipitation_mm"].clip(0, 20) / 20 * 80000 + 15000
            ).astype(int)
            rain["precip_color"] = rain["precipitation_mm"].apply(
                lambda mm: (
                    [0, 150, 255, 160]
                    if mm < 5
                    else [0, 80, 255, 190]
                    if mm < 15
                    else [0, 0, 200, 220]
                )
            )
            layers.append(
                pdk.Layer(
                    "ScatterplotLayer",
                    id="weather-precip",
                    data=rain,
                    get_position=["lon", "lat"],
                    get_radius="precip_radius",
                    get_fill_color="precip_color",
                    get_line_color=[0, 100, 255, 100],
                    line_width_min_pixels=1,
                    stroked=True,
                    filled=True,
                    pickable=False,
                )
            )

    if show_density and not flights.empty:
        density_data = flights[["lon", "lat"]].copy()
        density_data["lon"] = density_data["lon"].astype(float)
        density_data["lat"] = density_data["lat"].astype(float)
        density_data["w"] = 1
        layers.append(
            pdk.Layer(
                "HeatmapLayer",
                id="flight-density",
                data=density_data.to_dict("records"),
                get_position=["lon", "lat"],
                get_weight="w",
                radius_pixels=30,
                intensity=2.0,
                threshold=0.05,
                color_range=[
                    [0, 229, 255, 0],
                    [0, 150, 255, 60],
                    [100, 0, 255, 120],
                    [255, 0, 150, 180],
                    [255, 50, 0, 220],
                ],
                pickable=False,
            )
        )

    if show_routes and not routes.empty:
        route_data = routes[
            ["src_lon", "src_lat", "tgt_lon", "tgt_lat", "tooltip"]
        ].copy()
        for col in ["src_lon", "src_lat", "tgt_lon", "tgt_lat"]:
            route_data[col] = route_data[col].astype(float)
        layers.append(
            pdk.Layer(
                "ArcLayer",
                id="active-routes",
                data=route_data.to_dict("records"),
                get_source_position=["src_lon", "src_lat"],
                get_target_position=["tgt_lon", "tgt_lat"],
                get_source_color=[0, 229, 255, 40],
                get_target_color=[123, 97, 255, 40],
                get_width=1,
                width_min_pixels=1,
                pickable=True,
                auto_highlight=True,
            )
        )

    if show_airports and not airports.empty:
        ap_data = airports[["lon", "lat", "tooltip"]].copy()
        ap_data["lon"] = ap_data["lon"].astype(float)
        ap_data["lat"] = ap_data["lat"].astype(float)
        layers.append(
            pdk.Layer(
                "ScatterplotLayer",
                id="airports",
                data=ap_data.to_dict("records"),
                get_position=["lon", "lat"],
                get_radius=1000,
                get_fill_color=[255, 220, 0, 200],
                get_line_color=[255, 180, 0, 255],
                line_width_min_pixels=1,
                stroked=True,
                filled=True,
                pickable=True,
            )
        )

    if show_flights and not flights.empty:
        on_ground = flights[flights["on_ground"]]
        if not on_ground.empty:
            og_data = on_ground[["lon", "lat", "tooltip"]].copy()
            og_data["lon"] = og_data["lon"].astype(float)
            og_data["lat"] = og_data["lat"].astype(float)
            layers.append(
                pdk.Layer(
                    "ScatterplotLayer",
                    data=og_data.to_dict("records"),
                    get_position=["lon", "lat"],
                    get_radius=8000,
                    get_fill_color=[80, 200, 120, 160],
                    pickable=True,
                )
            )

        airborne = flights[~flights["on_ground"]]
        if not airborne.empty:
            airborne = airborne.copy()

            if show_tracks:

                def _project(row):
                    """Proyecta un punto adelante del avión según track y velocidad."""
                    dist_deg = max(0.3, min(3.0, row["velocity"] / 100))
                    rad = math.radians(row["true_track"])
                    return [
                        row["lon"] + dist_deg * math.sin(rad),
                        row["lat"] + dist_deg * math.cos(rad),
                    ]

                track_data = airborne[["lon", "lat", "true_track", "velocity"]].copy()
                track_data["lon"] = track_data["lon"].astype(float)
                track_data["lat"] = track_data["lat"].astype(float)
                track_data["true_track"] = track_data["true_track"].astype(float)
                track_data["velocity"] = track_data["velocity"].astype(float)
                track_data["target"] = track_data.apply(_project, axis=1)
                track_data["source"] = track_data.apply(
                    lambda r: [r["lon"], r["lat"]], axis=1
                )
                layers.append(
                    pdk.Layer(
                        "LineLayer",
                        id="flight-tracks",
                        data=track_data[["source", "target"]].to_dict("records"),
                        get_source_position="source",
                        get_target_position="target",
                        get_color=[0, 229, 255, 80],
                        get_width=1,
                        width_min_pixels=1,
                        pickable=False,
                    )
                )

            airborne["icon_data"] = [_ICON_DATA] * len(airborne)
            airborne["icon_angle"] = -airborne["angle"] + 45
            layers.append(
                pdk.Layer(
                    "IconLayer",
                    data=airborne,
                    get_icon="icon_data",
                    get_position=["lon", "lat"],
                    get_angle="icon_angle",
                    get_size=40,
                    size_scale=1,
                    pickable=True,
                    get_color=[0, 229, 255, 220],
                    parameters={"depthTest": False},
                )
            )

    if show_seismics and not seismics.empty:
        layers.extend(build_seismic_layers(seismics.head(10)))

    tooltip = {
        "html": """
            <div style="background-color: #080d14; color: #c8d8e8;
            padding: 10px; border: 1px solid #1a2a3a; border-radius: 4px;">
                {tooltip}
            </div>
        """,
        "style": {
            "backgroundColor": "transparent",
            "color": "white",
            "zIndex": "10001",
        },
    }

    return pdk.Deck(
        layers=layers,
        initial_view_state=_DEFAULT_VIEW,
        tooltip=tooltip,  # type: ignore[arg-type]
        map_style=_MAP_STYLE,
    )
