
from pethub.petfinder import animals, organizations, images


def main():
    animals.get_animals()
    images.download_images(table='animals')


if __name__ == '__main__':
    main()
