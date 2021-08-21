
from pethub.petfinder import animals, organizations, images


def main():
    try:
        animals.get_animals()
    except:
        pass
    images.download_images(table='animals')


if __name__ == '__main__':
    main()
