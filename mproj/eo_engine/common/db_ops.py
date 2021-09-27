from typing import TypedDict

from eo_engine.errors import AfriCultuReSFileNotExist, AfriCultuReSFileInUse
from eo_engine.models import EOSource, EOSourceStateChoices
from eo_engine.models import EOProduct, EOProductStateChoices

DeletedReport = TypedDict('DeletedReport', {'eo_source': int, 'eo_product': int})


def delete_eo_product(eo_product_pk: int) -> DeletedReport:
    eo_product = EOProduct.objects.get(pk=eo_product_pk)
    deb_eo_products = EOProduct.objects.filter(eo_products_inputs=eo_product)

    # file is missing
    if not bool(eo_product.file):
        raise AfriCultuReSFileNotExist("The file does not exist to delete.")

    # file is/will be used
    active_eo_products = deb_eo_products.filter(
        state__in=(EOProductStateChoices.Scheduled,
                   EOProductStateChoices.Generating))
    if active_eo_products.exists():
        raise AfriCultuReSFileInUse('Cannot delete the file. It is being used for a scheduled/on-going procedure')

    safe_to_delete_deb_eo_products = deb_eo_products.filter(
        state__in=(EOProductStateChoices.Failed,
                   EOProductStateChoices.Available)
    )
    deleted_eo_products = safe_to_delete_deb_eo_products.count()
    if safe_to_delete_deb_eo_products.exists():
        safe_to_delete_deb_eo_products.delete()

    # deal with self
    # Are all inputs present and available
    input_eo_source = eo_product.eo_sources_inputs.all()
    input_eo_product = eo_product.eo_products_inputs.all()

    safe_self_to_remove_row = False
    if input_eo_source.exists():
        if input_eo_source.count() == input_eo_source.filter(
                state=EOSourceStateChoices.AvailableLocally).count():
            # keep safe_self_to_remove_row false
            pass
        else:
            safe_self_to_remove_row = True

    if input_eo_product.exists():
        if input_eo_product.count() == input_eo_product.filter(
                state=EOProductStateChoices.Ready).count():
            # keep safe_self_to_remove_row false
            pass
        else:
            safe_self_to_remove_row = True

    eo_product.file.delete()
    if safe_self_to_remove_row:
        eo_product.delete()
    else:
        eo_product.state = EOProductStateChoices.Ignore
        eo_product.save()

    return {"eo_source": 1,
            # +1 because we delete self
            "eo_product": deleted_eo_products + 1
            }


def delete_eo_source(eo_source_pk: int) -> DeletedReport:
    """Delete EOSource file if made, and mark the row as IGNORED.
    Take care for depended EOProducts,

    Raises errors when file does not exists or a dep file is used
    """

    eo_source = EOSource.objects.get(pk=eo_source_pk)
    deb_eo_products = EOProduct.objects.filter(eo_products_inputs=eo_source)
    if not bool(eo_source.file):
        raise AfriCultuReSFileNotExist("The file does not exist to delete.")

    active_eo_products = deb_eo_products.filter(
        state__in=(EOProductStateChoices.Scheduled,
                   EOProductStateChoices.Generating))
    to_delete_eo_products = deb_eo_products.filter(
        state__in=(EOProductStateChoices.Available, EOProductStateChoices.Failed)
    )

    if active_eo_products.exists():
        raise AfriCultuReSFileInUse('Cannot delete the file. It is being used for a scheduled/on-going procedure')

    deleted_eo_products = to_delete_eo_products.count()
    to_delete_eo_products.delete()

    eo_source.file.delete(save=False)
    eo_source.state = EOSourceStateChoices.Ignore

    return {"eo_source": 1, "eo_product": deleted_eo_products}
