from __future__ import annotations
import os
import sys
import time
import glob
import logging
from pathlib import Path
from typing import Optional, Tuple, List
import dotenv

dotenv.load_dotenv(override=True)

import smtplib
import ssl
import mimetypes
from email.message import EmailMessage

import re
from typing import Iterable, Optional, List, Tuple
import pandas as pd
import numpy as np
from html import escape as html_escape

import pandas as pd

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, NoSuchElementException, WebDriverException
)

# =========================
# Configuración de logging
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("supercias")

URL = "https://mercadodevalores.supercias.gob.ec/reportes/directorioCompanias.jsf"
OUTPUT_FILE = "companias_constituidas_ultima_fecha.xlsx"
CIIU_FILE_CANDIDATES = ["CIIU.xls", "CIIU.xlsx"]  # probamos ambos formatos


# =========================
# Utilidades de paths
# =========================
def working_dir() -> Path:
    """Devuelve el directorio de trabajo estable (soporta __file__ y ejecución directa)."""
    try:
        return Path(__file__).resolve().parent
    except NameError:
        return Path.cwd()

def cleanup_excels(directory: Path) -> None:
    """
    Borra cualquier archivo Excel previo excepto el de CIIU.
    Incluye tanto los directorio_companias como el archivo de salida.
    """
    patterns = ["directorio*.xls", "directorio*.xlsx", OUTPUT_FILE]
    removed = []
    for pat in patterns:
        for f in directory.glob(pat):
            # Evitamos borrar CIIU.xls o CIIU.xlsx
            if f.name.lower().startswith("ciiu"):
                continue
            try:
                f.unlink()
                removed.append(f.name)
            except Exception as e:
                logger.warning("No se pudo borrar %s: %s", f, e)
    if removed:
        logger.info("Archivos Excel eliminados antes de iniciar: %s", ", ".join(removed))


# =========================
# Selenium
# =========================
def build_driver(download_dir: Path) -> webdriver.Chrome:
    """
    Inicializa Chrome con descarga automática en download_dir y menos ruido de logs.
    """
    options = webdriver.ChromeOptions()

    prefs = {
        "download.default_directory": str(download_dir),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    options.add_experimental_option("prefs", prefs)

    # Reducir ruido de consola de Chrome
    options.add_experimental_option("excludeSwitches", ["enable-logging", "enable-automation"])
    options.add_argument("--disable-features=Translate,MediaRouter,OptimizationHints,AutofillServerCommunication")
    options.add_argument("--headless=new")  # modo headless
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--log-level=3")
    options.add_argument("--window-size=1280,1024")

    try:
        driver = webdriver.Chrome(options=options, service=ChromeService())
        return driver
    except WebDriverException as e:
        logger.error("No se pudo iniciar ChromeDriver. Revisa la versión del driver/navegador: %s", e)
        raise


def wait_for_text(driver: webdriver.Chrome, by: Tuple[By, str], timeout: int = 15) -> str:
    """Espera un elemento y devuelve su .text."""
    try:
        elem = WebDriverWait(driver, timeout).until(EC.presence_of_element_located(by))
        return elem.text.strip()
    except TimeoutException:
        raise TimeoutException(f"No se encontró el elemento para extraer texto con selector: {by}")


def click_download_link(driver: webdriver.Chrome, timeout: int = 20) -> None:
    """
    Intenta clicar el enlace de descarga del Excel usando selectores robustos.
    Ajusta si la página cambia.
    """
    wait = WebDriverWait(driver, timeout)

    # 1) Intento por link text visible (parcial), p.ej. "Excel"
    try:
        link = wait.until(EC.element_to_be_clickable((By.PARTIAL_LINK_TEXT, "Excel")))
        link.click()
        return
    except TimeoutException:
        logger.warning("No se encontró link por texto parcial 'Excel'. Probando por href...")

    # 2) Fallback por href que contenga .xlsx o .xls
    xpaths = [
        "//a[contains(@href,'.xlsx')]",
        "//a[contains(@href,'.xls')]",
        # Como último recurso, algún botón dentro de un bloque con 'descargar'
        "//*[self::a or self::button][contains(translate(., 'DESCARGAR', 'descargar'),'descargar')]",
    ]
    for xp in xpaths:
        try:
            link = wait.until(EC.element_to_be_clickable((By.XPATH, xp)))
            link.click()
            return
        except TimeoutException:
            continue

    raise TimeoutException("No pude encontrar el enlace/botón para descargar el Excel.")


def newest_file_in_dir(directory: Path, patterns: List[str]) -> Optional[Path]:
    """Devuelve el archivo más reciente en 'directory' que matchee cualquiera de los patterns."""
    files = []
    for pat in patterns:
        files.extend(glob.glob(str(directory / pat)))
    if not files:
        return None
    paths = [Path(f) for f in files]
    return max(paths, key=lambda p: p.stat().st_mtime)


def wait_for_download(directory: Path, timeout: int = 120, *, exclude_names: List[str] = None, min_mtime: float = 0.0) -> Path:
    """
    Espera a que aparezca un archivo .xls/.xlsx nuevo (mtime > min_mtime), que no sea .crdownload
    ni esté en la lista de excluidos. Devuelve la ruta del archivo descargado.
    """
    exclude_names = set(exclude_names or [])
    end = time.time() + timeout
    logger.info("Esperando la descarga del Excel...")

    while time.time() < end:
        # Preferimos archivos que parezcan el directorio oficial (directorio_*.xls/xlsx)
        candidates = []
        for pat in ["directorio*.xlsx", "directorio*.xls", "*.xlsx", "*.xls", "*.XLSX", "*.XLS"]:
            candidates.extend(Path(directory).glob(pat))

        # Filtrar válidos
        valid = []
        for p in candidates:
            if p.name in exclude_names:
                continue
            if p.name.endswith(".crdownload"):
                continue
            try:
                mtime = p.stat().st_mtime
            except FileNotFoundError:
                continue
            if mtime <= min_mtime:
                continue
            valid.append((mtime, p))

        if valid:
            # Tomamos el más nuevo
            valid.sort(key=lambda t: t[0], reverse=True)
            picked = valid[0][1]
            # Asegurar que no tenga el temporal .crdownload al lado
            if not picked.with_suffix(picked.suffix + ".crdownload").exists():
                logger.info("Descarga detectada: %s", picked.name)
                return picked

        time.sleep(0.4)

    raise TimeoutException("Timeout esperando la descarga del Excel (.xls/.xlsx).")


def download_excel(driver: webdriver.Chrome, url: str, download_dir: Path) -> Tuple[Optional[str], Path]:
    """
    Navega, recupera la 'fecha de actualización' y descarga el Excel.
    Devuelve (fecha_actualizacion_base, ruta_excel_descargado).
    """
    driver.get(url)

    # Snapshot de archivos existentes y su mtime para ignorarlos
    pre_existing = {p.name: p.stat().st_mtime for p in download_dir.glob("*") if p.is_file()}

    # Fecha (heurístico). Si no la encuentra, seguimos.
    fecha_text: Optional[str] = None
    try:
        fecha_text = wait_for_text(
            driver,
            (By.XPATH, "//*[self::label or self::span][contains(., 'Actualización') or contains(., 'actualización')]"),
            timeout=10
        )
    except TimeoutException:
        logger.warning("No se pudo capturar 'Fecha de actualización' con el selector heurístico.")

    click_download_link(driver, timeout=25)

    # Excluir el archivo de salida y cualquier preexistente
    exclude = [OUTPUT_FILE] + list(pre_existing.keys())
    min_mtime = max(pre_existing.values(), default=0.0)

    excel_path = wait_for_download(download_dir, timeout=180, exclude_names=exclude, min_mtime=min_mtime)
    return fecha_text, excel_path


# =========================
# Carga y transformación
# =========================
def read_excel_any(path: Path) -> pd.DataFrame:
    """
    Lee Excel ya sea .xlsx (openpyxl) o .xls (xlrd).
    Lanza un error amable si falta el engine.
    """
    suffix = path.suffix.lower()
    if suffix == ".xlsx":
        try:
            return pd.read_excel(path, engine="openpyxl")
        except Exception as e:
            raise RuntimeError(f"No pude leer {path.name} como .xlsx (openpyxl). Detalle: {e}")
    elif suffix == ".xls":
        try:
            # xlrd >= 2.0 no soporta .xls: hay que tener xlrd==1.2.0 o equivalente
            return pd.read_excel(path, engine="xlrd")
        except Exception as e:
            raise RuntimeError(
                f"No pude leer {path.name} como .xls. Instala 'xlrd==1.2.0' o "
                f"convierte el archivo a .xlsx. Detalle: {e}"
            )
    else:
        raise ValueError(f"Extensión no soportada: {suffix}")


def load_companies(excel_path: Path) -> pd.DataFrame:
    """
    Carga el directorio de compañías.
    1) Intenta como antes: skiprows=4.
    2) Si falla, intenta offsets alternativos.
    """
    expected = {"EXPEDIENTE", "RUC", "NOMBRE", "FECHA_CONSTITUCION"}

    # 1) Lógica original
    try:
        df = pd.read_excel(
            excel_path,
            skiprows=4,
            engine="openpyxl" if excel_path.suffix.lower()==".xlsx" else None
        )
        if expected.issubset(set(map(str, df.columns))):
            return df
    except Exception:
        pass

    # 2) Fallbacks conservadores
    for skip in (0, 1, 2, 3, 5):
        try:
            tmp = pd.read_excel(
                excel_path,
                skiprows=skip,
                engine="openpyxl" if excel_path.suffix.lower()==".xlsx" else None
            )
            if expected.issubset(set(map(str, tmp.columns))):
                return tmp
        except Exception:
            continue

    raise RuntimeError("No se encontraron las columnas esperadas en el Excel de compañías. Revisa el layout.")



def load_ciiu(base_dir: Path) -> pd.DataFrame:
    """
    Carga el catálogo CIIU desde CIIU.xls o CIIU.xlsx en el directorio.
    Requiere columnas ['Código actividad económica', 'Descripción actividad económica'].
    """
    ciiu_path = None
    for fname in CIIU_FILE_CANDIDATES:
        p = base_dir / fname
        if p.exists():
            ciiu_path = p
            break
    if not ciiu_path:
        raise FileNotFoundError(
            f"No se encontró ningún archivo de CIIU en {base_dir}. "
            f"Busca uno con nombre {CIIU_FILE_CANDIDATES}."
        )

    # Muchas hojas CIIU tienen encabezado en 2 filas
    df = read_excel_any(ciiu_path)
    # Heurística: si tiene columnas genéricas, intenta levantar encabezado desde filas iniciales
    if not {"Código actividad económica", "Descripción actividad económica"}.issubset(df.columns):
        # intentar skiprows=2 como tu ejemplo
        try:
            df = pd.read_excel(ciiu_path, skiprows=2, engine="openpyxl" if ciiu_path.suffix.lower()==".xlsx" else "xlrd")
        except Exception:
            pass

    # Filtrar a dos columnas esperadas (renombrando con seguridad de espacios)
    cols_map = {c.strip(): c for c in df.columns}
    code_col = cols_map.get("Código actividad económica")
    desc_col = cols_map.get("Descripción actividad económica")
    if not code_col or not desc_col:
        raise RuntimeError(
            "El archivo CIIU no contiene las columnas 'Código actividad económica' y 'Descripción actividad económica'."
        )
    df = df[[code_col, desc_col]].rename(columns={code_col: "CODIGO", desc_col: "DESCRIPCION"})
    # Normalizar código sin puntos
    df["CODIGO"] = df["CODIGO"].astype(str).str.replace(".", "", regex=False).str.strip()
    return df


def _resolve_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    """
    Devuelve el nombre real de columna que coincida con alguno de 'candidates'
    (comparación casefold y sin dobles espacios). Si no hay match, None.
    """
    norm = {str(c).strip().casefold(): c for c in df.columns}
    for cand in candidates:
        k = cand.strip().casefold()
        if k in norm:
            return norm[k]
    return None


def transform_companies(companies: pd.DataFrame, ciiu: pd.DataFrame) -> Tuple[pd.DataFrame, pd.Timestamp]:
    df = companies.copy()

    # 1) Fecha
    if "FECHA_CONSTITUCION" not in df.columns:
        raise RuntimeError("No existe la columna 'FECHA_CONSTITUCION' en el Excel de compañías.")
    df["FECHA_CONSTITUCION"] = pd.to_datetime(df["FECHA_CONSTITUCION"], dayfirst=True, errors="coerce")
    if df["FECHA_CONSTITUCION"].isna().all():
        raise RuntimeError("No pude parsear 'FECHA_CONSTITUCION'. Revisa el formato de fechas.")
    ultima_fecha = df["FECHA_CONSTITUCION"].max()
    df = df.loc[df["FECHA_CONSTITUCION"] == ultima_fecha].reset_index(drop=True)

    # 2) Resolver columnas CIIU (mantiene tu lógica original)
    col_ciiu6 = _resolve_col(df, ["CIIU NIVEL 6"])
    col_ciiu1 = _resolve_col(df, ["CIIU NIVEL 1"])

    if col_ciiu6:
        df.loc[:, col_ciiu6] = df[col_ciiu6].astype(str).str.replace(".", "", regex=False).str.strip()
    else:
        logger.warning("No se encontró la columna 'CIIU NIVEL 6' en compañías. El merge correspondiente quedará vacío.")

    if col_ciiu1:
        df.loc[:, col_ciiu1] = df[col_ciiu1].astype(str).str.replace(".", "", regex=False).str.strip()
    else:
        logger.warning("No se encontró la columna 'CIIU NIVEL 1' en compañías. El merge correspondiente quedará vacío.")

    # 3) Merges condicionados
    if col_ciiu6:
        df = df.merge(ciiu.add_prefix("N6_"), left_on=col_ciiu6, right_on="N6_CODIGO", how="left")
    else:
        # Si falta, crea columnas vacías esperadas para que más abajo existan
        df["N6_DESCRIPCION"] = pd.NA

    if col_ciiu1:
        df = df.merge(ciiu.add_prefix("N1_"), left_on=col_ciiu1, right_on="N1_CODIGO", how="left")
    else:
        df["N1_DESCRIPCION"] = pd.NA

    # 4) Selección/renombres
    want_cols = [
        'EXPEDIENTE', 'RUC', 'NOMBRE', 'FECHA_CONSTITUCION', 'TIPO', 'PAÍS', 'REGIÓN', 'PROVINCIA',
        'CANTÓN', 'CIUDAD', 'CALLE', 'NÚMERO', 'INTERSECCIÓN', 'BARRIO', 'TELÉFONO',
        'REPRESENTANTE', 'CARGO', 'CAPITAL SUSCRITO',
        'N6_DESCRIPCION',  # actividad económica
        'N1_DESCRIPCION',  # actividad principal
    ]
    existing = [c for c in want_cols if c in df.columns]
    df = df[existing].copy()

    df = df.rename(columns={
        'N6_DESCRIPCION': 'ACTIVIDAD_ECONÓMICA',
        'N1_DESCRIPCION': 'ACTIVIDAD_PRINCIPAL',
    })

    # 5) Title Case en columnas de texto
    obj_cols = df.select_dtypes(include=["object"]).columns
    # pandas >= 2.2: DataFrame.map reemplaza applymap
    df[obj_cols] = df[obj_cols].map(lambda s: s.title() if isinstance(s, str) else s)

    return df, ultima_fecha




def save_output(df: pd.DataFrame, output_path: Path) -> None:
    try:
        df.to_excel(output_path, index=False, engine="openpyxl")
        logger.info("Archivo generado: %s", output_path.name)
    except Exception as e:
        raise RuntimeError(f"No se pudo guardar el archivo de salida: {e}")


# ------------------------------------------------------------
# Utilidades de formateo (números y tablas HTML amigables)
# ------------------------------------------------------------
def _format_int_es(x: Optional[float | int]) -> str:
    if pd.isna(x):
        return ""
    try:
        x = int(round(float(x)))
    except Exception:
        return ""
    # miles con punto (formato ES)
    return f"{x:,}".replace(",", ".")  # 1234567 -> 1.234.567

def _format_money_usd_es(x: Optional[float]) -> str:
    if pd.isna(x):
        return ""
    try:
        v = float(x)
    except Exception:
        return ""
    s = f"{v:,.2f}"  # 1,234,567.89
    s = s.replace(",", "X").replace(".", ",").replace("X", ".")  # -> 1.234.567,89
    return f"USD {s}"

def _table_html(headers: List[str], rows: Iterable[Iterable[str]]) -> str:
    # CSS minimalista compatible con email
    css = (
        "border-collapse:collapse;width:100%;"
        "font-family:Arial,Helvetica,sans-serif;font-size:13px;"
    )
    th_css = (
        "background:#f3f4f6;color:#111827;text-align:left;"
        "padding:8px;border:1px solid #e5e7eb;"
    )
    td_css = "padding:8px;border:1px solid #e5e7eb;color:#111827;"
    html = [f'<table style="{css}" role="presentation" cellspacing="0" cellpadding="0">']
    html.append("<thead><tr>")
    for h in headers:
        html.append(f'<th style="{th_css}">{html_escape(h)}</th>')
    html.append("</tr></thead>")
    html.append("<tbody>")
    for r in rows:
        html.append("<tr>")
        for c in r:
            html.append(f'<td style="{td_css}">{c}</td>')
        html.append("</tr>")
    html.append("</tbody></table>")
    return "".join(html)

# ------------------------------------------------------------
# Parseo robusto de "CAPITAL SUSCRITO" (acepta str o numérico)
# ------------------------------------------------------------
def _to_number_series(s: pd.Series) -> pd.Series:
    if s.dtype.kind in "if":
        return s.astype(float)
    # eliminar cualquier texto no numérico y normalizar separadores
    cleaned = (
        s.astype(str)
         .str.replace(r"[^\d,.\-]", "", regex=True)
         .str.replace(r"(?<=\d)[,](?=\d{3}\b)", "", regex=True)  # quita comas de miles
    )
    # intenta: primero puntos de miles, coma decimal -> cambia coma por punto
    try1 = pd.to_numeric(cleaned.str.replace(",", "."), errors="coerce")
    # si quedaron muchos NaN, intenta eliminar todos los separadores
    if try1.isna().mean() > 0.6:
        try2 = pd.to_numeric(cleaned.str.replace(",", "").str.replace(".", ""), errors="coerce")
        return try2
    return try1

# ------------------------------------------------------------
# Generador principal de HTML
# ------------------------------------------------------------
def build_daily_email(
    df: pd.DataFrame,
    *,
    titulo: str = "Resumen Diario de Nuevas Compañías",
    fuente: str = "Superintendencia de Compañías",
    top_actividades: int = 15,
    top_ciudades: Optional[int] = None,  # None = todas
    capital_bins: Optional[List[float]] = None,
    capital_labels: Optional[List[str]] = None,
) -> str:
    """
    df: DataFrame con columnas:
        ['EXPEDIENTE','RUC','NOMBRE','FECHA_CONSTITUCION','TIPO','PAÍS','REGIÓN','PROVINCIA',
         'CANTÓN','CIUDAD','CALLE','NÚMERO','INTERSECCIÓN','BARRIO','TELÉFONO','REPRESENTANTE',
         'CARGO','CAPITAL SUSCRITO','ACTIVIDAD_ECONÓMICA','ACTIVIDAD_PRINCIPAL']
    """
    required = {
        'EXPEDIENTE','RUC','NOMBRE','FECHA_CONSTITUCION','CIUDAD',
        'CAPITAL SUSCRITO','ACTIVIDAD_PRINCIPAL'
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Faltan columnas requeridas en df: {sorted(missing)}")

    data = df.copy()

    # Normalizaciones
    data['CIUDAD'] = data['CIUDAD'].fillna("No especificada").astype(str).str.strip().str.title()
    data['ACTIVIDAD_PRINCIPAL'] = (
        data['ACTIVIDAD_PRINCIPAL'].fillna("No especificada").astype(str).str.strip().str.title()
    )

    # Fecha de constitución (asume que ya es una sola fecha; tomamos la máx por robustez)
    fechas_validas = pd.to_datetime(data['FECHA_CONSTITUCION'], errors='coerce', dayfirst=True)
    fecha_ref = fechas_validas.max()
    fecha_str = fecha_ref.strftime("%d/%m/%Y") if pd.notna(fecha_ref) else "N/D"

    # Capital numérico
    data['_CAPITAL_NUM'] = _to_number_series(data['CAPITAL SUSCRITO']).fillna(0.0)

    # Totales
    total_empresas = int(len(data))
    total_capital = data['_CAPITAL_NUM'].sum()

    # --------------------------------------------------------
    # 1) Por ciudad
    # --------------------------------------------------------
    por_ciudad = (
        data.groupby('CIUDAD', dropna=False)
            .size()
            .reset_index(name='Empresas')
            .sort_values(['Empresas','CIUDAD'], ascending=[False, True], kind="mergesort")
            .reset_index(drop=True)
    )
    if top_ciudades is not None and top_ciudades > 0 and len(por_ciudad) > top_ciudades:
        top = por_ciudad.iloc[:top_ciudades].copy()
        resto_emp = por_ciudad.iloc[top_ciudades:]['Empresas'].sum()
        resto_row = pd.DataFrame([{'CIUDAD': 'Otras', 'Empresas': resto_emp}])
        por_ciudad_vista = pd.concat([top, resto_row], ignore_index=True)
    else:
        por_ciudad_vista = por_ciudad.copy()

    ciudad_headers = ["Ciudad", "Empresas"]
    ciudad_rows = [
        [html_escape(r['CIUDAD']), _format_int_es(r['Empresas'])]
        for _, r in por_ciudad_vista.iterrows()
    ]
    ciudad_table = _table_html(ciudad_headers, ciudad_rows)

    # --------------------------------------------------------
    # 2) Clasificación por capital suscrito (bins)
    # --------------------------------------------------------
    if capital_bins is None:
        # USD: 0–10k, 10–50k, 50–100k, 100–500k, 0.5–1M, 1–5M, 5–10M, 10M+
        capital_bins = [0, 10_000, 50_000, 100_000, 500_000, 1_000_000, 5_000_000, 10_000_000, np.inf]
    if capital_labels is None:
        capital_labels = [
            "0 – 10 mil", "10 – 50 mil", "50 – 100 mil", "100 – 500 mil",
            "0,5 – 1 M", "1 – 5 M", "5 – 10 M", "10 M+"
        ]
    data['_CAP_BIN'] = pd.cut(data['_CAPITAL_NUM'], bins=capital_bins, labels=capital_labels, include_lowest=True, right=True)

    cap_grp = (
        data.groupby('_CAP_BIN', dropna=False)
            .agg(Empresas=('RUC','count'),
                 Capital_Total=('_CAPITAL_NUM','sum'),
                 Capital_Promedio=('_CAPITAL_NUM','mean'))
            .reset_index()
    )
    # orden en el mismo de labels
    cap_grp['_order'] = cap_grp['_CAP_BIN'].apply(lambda x: capital_labels.index(str(x)) if pd.notna(x) else 9_999)
    cap_grp = cap_grp.sort_values('_order').drop(columns=['_order'])

    cap_headers = ["Rango de Capital Suscrito", "Empresas", "Capital Total", "Capital Promedio"]
    cap_rows = [
        [
            html_escape(str(r['_CAP_BIN']) if pd.notna(r['_CAP_BIN']) else "No clasificado"),
            _format_int_es(r['Empresas']),
            _format_money_usd_es(r['Capital_Total']),
            _format_money_usd_es(r['Capital_Promedio']),
        ]
        for _, r in cap_grp.iterrows()
    ]
    cap_table = _table_html(cap_headers, cap_rows)

    # --------------------------------------------------------
    # 3) Por actividad principal
    # --------------------------------------------------------
    act_grp = (
        data.groupby('ACTIVIDAD_PRINCIPAL', dropna=False)
            .agg(Empresas=('RUC','count'), Capital_Total=('_CAPITAL_NUM','sum'))
            .reset_index()
            .sort_values(['Empresas','Capital_Total'], ascending=[False, False], kind="mergesort")
    )
    if top_actividades and len(act_grp) > top_actividades:
        act_vista = act_grp.iloc[:top_actividades].copy()
    else:
        act_vista = act_grp.copy()

    act_headers = ["Actividad Principal", "Empresas", "Capital Total"]
    act_rows = [
        [html_escape(r['ACTIVIDAD_PRINCIPAL']), _format_int_es(r['Empresas']), _format_money_usd_es(r['Capital_Total'])]
        for _, r in act_vista.iterrows()
    ]
    act_table = _table_html(act_headers, act_rows)

    # --------------------------------------------------------
    # HTML final
    # --------------------------------------------------------
    wrapper_css = (
        "max-width:900px;margin:0 auto;padding:16px;"
        "font-family:Arial,Helvetica,sans-serif;color:#111827;line-height:1.45;"
    )
    h1_css = "font-size:18px;margin:0 0 6px 0;"
    p_css = "margin:6px 0;"
    hr_css = "border:none;border-top:1px solid #e5e7eb;margin:16px 0;"

    total_emp = _format_int_es(total_empresas)
    total_cap = _format_money_usd_es(total_capital)

    html = []
    html.append(f'<div style="{wrapper_css}">')
    html.append(f'<h1 style="{h1_css}">{html_escape(titulo)}</h1>')
    html.append(f'<p style="{p_css}"><strong>Fecha de constitución considerada:</strong> {html_escape(fecha_str)}</p>')
    html.append(f'<p style="{p_css}"><strong>Total de empresas encontradas:</strong> {total_emp} '
                f'&nbsp;&nbsp;|&nbsp;&nbsp; <strong>Capital total:</strong> {total_cap}</p>')
    html.append(f'<p style="{p_css}">Se adjuntan las últimas compañías registradas en la {html_escape(fuente)}.</p>')

    html.append(f'<hr style="{hr_css}">')
    html.append(f'<h2 style="{h1_css}">Distribución por ciudad</h2>')
    html.append(ciudad_table)

    html.append(f'<hr style="{hr_css}">')
    html.append(f'<h2 style="{h1_css}">Clasificación por capital suscrito</h2>')
    html.append(cap_table)

    html.append(f'<hr style="{hr_css}">')
    html.append(f'<h2 style="{h1_css}">Actividad principal</h2>')
    if top_actividades and len(act_grp) > top_actividades:
        html.append(f'<p style="{p_css}">Se muestran las <strong>{top_actividades}</strong> actividades con más empresas.</p>')
    html.append(act_table)

    html.append(f'<hr style="{hr_css}">')
    html.append(f'<p style="{p_css};color:#6b7280;font-size:12px">Fuente: {html_escape(fuente)}. '
                f'Este correo fue generado automáticamente.</p>')
    html.append("</div>")

    return "".join(html)

def send_daily_email(
    *,
    html_body: str,
    subject: str,
    attachments: list[Path] | None = None,
    host: str = None,
    port: int = None,
    username: str = None,
    password: str = None,
    from_addr: str = None,
    to_addrs: list[str] | None = None,
    cc_addrs: list[str] | None = None,
    bcc_addrs: list[str] | None = None,
    retries: int = 3,
    backoff_seconds: float = 2.0,
) -> None:
    """
    Envía un correo HTML con adjuntos. LEE configuración por variables de entorno si no se pasan:
      SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, SMTP_FROM, SMTP_TO, SMTP_CC, SMTP_BCC
    - SMTP_TO/CC/BCC: separar múltiples correos por coma.
    """
    # --- cargar config de entorno si faltan ---
    host = host or os.getenv("SMTP_HOST", "smtp.hostinger.com")
    port = port or int(os.getenv("SMTP_PORT", "465"))
    username = username or os.getenv("SMTP_USER")  # p.ej. no-reply@nexogest.io
    password = password or os.getenv("SMTP_PASS")
    from_addr = from_addr or os.getenv("SMTP_FROM", username or "")
    to_addrs = to_addrs or [a.strip() for a in os.getenv("SMTP_TO", "").split(",") if a.strip()]
    cc_addrs = cc_addrs or [a.strip() for a in os.getenv("SMTP_CC", "").split(",") if a.strip()]
    bcc_addrs = bcc_addrs or [a.strip() for a in os.getenv("SMTP_BCC", "").split(",") if a.strip()]
    attachments = attachments or []

    if not username or not password:
        raise RuntimeError("Faltan credenciales SMTP (SMTP_USER/SMTP_PASS).")
    if not to_addrs:
        raise RuntimeError("No hay destinatarios. Define SMTP_TO o pasa to_addrs.")

    # --- construir mensaje ---
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = ", ".join(to_addrs)
    if cc_addrs:
        msg["Cc"] = ", ".join(cc_addrs)

    # texto plano de respaldo
    plain_text = (
        "Resumen Diario – Nuevas Compañías\n\n"
        "Se adjuntan las últimas compañías registradas en la Superintendencia de Compañías.\n"
        "Abra el correo en un cliente compatible con HTML para ver el detalle."
    )
    msg.set_content(plain_text)
    msg.add_alternative(html_body, subtype="html")

    # --- adjuntos ---
    for path in attachments:
        try:
            mime_type, _ = mimetypes.guess_type(path.name)
            maintype, subtype = (mime_type or "application/octet-stream").split("/", 1)
            with open(path, "rb") as fh:
                msg.add_attachment(
                    fh.read(),
                    maintype=maintype,
                    subtype=subtype,
                    filename=path.name,
                )
        except FileNotFoundError:
            logger.warning("Adjunto no encontrado, se omite: %s", path)
        except Exception as e:
            logger.warning("No se pudo adjuntar %s: %s", path, e)

    # --- enviar con reintentos ---
    all_recipients = to_addrs + cc_addrs + bcc_addrs
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(host, port, context=context, timeout=30) as server:
                server.login(username, password)
                server.send_message(msg, from_addr=from_addr, to_addrs=all_recipients)
            logger.info("Correo enviado a: %s", ", ".join(all_recipients))
            return
        except Exception as e:
            last_err = e
            logger.warning("Intento %d de envío fallido: %s", attempt, e)
            if attempt < retries:
                time.sleep(backoff_seconds * attempt)

    raise RuntimeError(f"No se pudo enviar el correo tras {retries} intentos: {last_err}")


# =========================
# Programa principal
# =========================
def main() -> None:
    base_dir = working_dir()
    download_dir = base_dir  # guardamos en el directorio de ejecución
    cleanup_excels(download_dir)

    driver = None
    try:
        driver = build_driver(download_dir)
        fecha_actualizacion, excel_path = download_excel(driver, URL, download_dir)

        # Cargar datos
        companies = load_companies(excel_path)
        ciiu = load_ciiu(base_dir)

        # Transformar
        df_out, ultima_fecha = transform_companies(companies, ciiu)

        # Guardar
        save_output(df_out, base_dir / OUTPUT_FILE)

        # Mensaje final
        if fecha_actualizacion:
            print(f"Fecha de Actualización de la Base de Datos (capturada): {fecha_actualizacion}")
        print(f"Última FECHA_CONSTITUCION detectada: {ultima_fecha.strftime('%d/%m/%Y')}")

    except Exception as e:
        logger.exception("Error durante la ejecución: %s", e)
        sys.exit(1)
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass
    email_html = build_daily_email(df_out,
                                titulo="Resumen Diario – Nuevas Compañías",
                                top_actividades=15,
                                top_ciudades=None)  # o un número, p.ej. 20
    #generar archivo html
    with open("reporte_diario.html", "w", encoding="utf-8") as f:
        f.write(email_html)

    email_html = build_daily_email(
        df_out,
        titulo="Resumen Diario – Nuevas Compañías",
        top_actividades=15,
        top_ciudades=None
    )

    # generar archivo html
    html_path = base_dir / "reporte_diario.html"
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(email_html)

    # --- ENVIAR CORREO ---
    try:
        send_daily_email(
            html_body=email_html,
            subject=f"Resumen Diario – Nuevas Compañías ({ultima_fecha.strftime('%d/%m/%Y')})",
            attachments=[base_dir / OUTPUT_FILE, html_path],
            # Si usas variables de entorno, no necesitas pasar nada más.
            # host="smtp.hostinger.com", port=465,
            # username="no-reply@nexogest.io", password="***",
            # from_addr="Nexogest Reportes <no-reply@nexogest.io>",
            # to_addrs=["destinatario@dominio.com"],
        )
    except Exception as e:
        logger.exception("Fallo al enviar el correo: %s", e)

if __name__ == "__main__":
    main()
