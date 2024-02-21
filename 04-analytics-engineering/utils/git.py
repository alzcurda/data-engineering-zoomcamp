import requests
import re

def get_releases(usuario,repositorio):
    url = f"https://api.github.com/repos/{usuario}/{repositorio}/releases"
    headers = ""

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        releases = response.json()
        for release in releases:
            print(f"Release ID: {release['id']}, Nombre: {release['name']}")

        return releases
    else:
        print(f"Error al obtener la lista de releases (código de estado {response.status_code}): {response.text}")

def obtener_enlaces_descarga(usuario, repositorio, id_release, token=None):
    url = f"https://api.github.com/repos/{usuario}/{repositorio}/releases/{id_release}"
    headers = {"Authorization": f"Bearer {token}"} if token else {}

    # Realizar la solicitud a la API de GitHub
    response = requests.get(url, headers=headers)

    # Verificar si la solicitud fue exitosa (código de estado 200)
    if response.status_code == 200:
        data = response.json()

        # Obtener los enlaces de descarga de cada activo
        enlaces_descarga = [asset["browser_download_url"] for asset in data.get("assets", [])]

        return enlaces_descarga
    else:
        print(f"Error al obtener el release (código de estado {response.status_code}): {response.text}")
        return []


def filtrar_por_patron(lista, patron):
    # Usar list comprehension para filtrar elementos que coinciden con el patrón
    elementos_filtrados = [elemento for elemento in lista if re.search(patron, elemento)]

    return elementos_filtrados

def obtener_enlaces_descarga_filtrados(usuario, repositorio, id_release, patron, token=None):
    # Obtener los enlaces de descarga
    enlaces_descarga = obtener_enlaces_descarga(usuario, repositorio, id_release)
    # Filtramos valores
    enlaces_filtrados = filtrar_por_patron(enlaces_descarga,patron)
    # Imprimir los enlaces de descarga
    for enlace in enlaces_filtrados:
        print(enlace)
    print(enlaces_filtrados)

    return enlaces_filtrados


if __name__ == "__main__":
    usuario = "DataTalksClub"
    repositorio = "nyc-tlc-data"
    release_yellow = 71974786
    release_green = 71979983
    release_fhv = 72017740
    patron = r"2019|2020"
    patron_fhv = r"2019"

    releases = get_releases (usuario,repositorio)

    # enlaces_filtrados = obtener_enlaces_descarga_filtrados(usuario, repositorio, release_yellow, patron)
    # enlaces_filtrados = obtener_enlaces_descarga_filtrados(usuario, repositorio, release_green, patron)
    enlaces_filtrados = obtener_enlaces_descarga_filtrados(usuario, repositorio, release_fhv, patron_fhv)

