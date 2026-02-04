# Copyright (c) OpenDataLab
# This file is derived from the MinerU project:
# https://github.com/opendatalab/MinerU
#
# Modifications and adaptations have been made by the Flash-MinerU authors.
#
# This file is licensed under the GNU Affero General Public License v3.0 (AGPL-3.0).
# You may obtain a copy of the License at:
#     https://www.gnu.org/licenses/agpl-3.0.html

from loguru import logger
from ..model.ocr.paddleocr2pytorch.pytorch_paddle import PytorchPaddleOCR

def ocr_model_init(det_db_box_thresh=0.3,
                   lang=None,
                   det_db_unclip_ratio=1.8,
                   enable_merge_det_boxes=True
                   ):
    if lang is not None and lang != '':
        model = PytorchPaddleOCR(
            det_db_box_thresh=det_db_box_thresh,
            lang=lang,
            use_dilation=True,
            det_db_unclip_ratio=det_db_unclip_ratio,
            enable_merge_det_boxes=enable_merge_det_boxes,
        )
    else:
        model = PytorchPaddleOCR(
            det_db_box_thresh=det_db_box_thresh,
            use_dilation=True,
            det_db_unclip_ratio=det_db_unclip_ratio,
            enable_merge_det_boxes=enable_merge_det_boxes,
        )
    return model

def atom_ocr_model_init(**kwargs):
    atom_model = None

    atom_model = ocr_model_init(
        kwargs.get('det_db_box_thresh', 0.3),
        kwargs.get('lang'),
        kwargs.get('det_db_unclip_ratio', 1.8),
        kwargs.get('enable_merge_det_boxes', True)
    )

    if atom_model is None:
        logger.error('model init failed')
        exit(1)
    else:
        return atom_model