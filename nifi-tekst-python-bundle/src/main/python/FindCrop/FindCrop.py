import time

from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators, ExpressionLanguageScope

try:
    from .find_crop import find_crop
    from .image_utils import read_image
    from .xmp_utils import transform_corners_to_deskewed, snap_bounds_to_known_size, build_xmp
    from .alto_utils import parse_alto_page_dimensions, alto_to_pixels, NB_ACCESS_SCALE
except ImportError:
    from find_crop import find_crop
    from image_utils import read_image
    from xmp_utils import transform_corners_to_deskewed, snap_bounds_to_known_size, build_xmp
    from alto_utils import parse_alto_page_dimensions, alto_to_pixels, NB_ACCESS_SCALE


class FindCrop(FlowFileTransform):

    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '1.0.0'
        description = (
            'Locates a cropped image within its uncropped original using template matching. '
            'Writes an XMP sidecar (crs: namespace) encoding the deskew angle and crop bounds '
            'sufficient to recreate the crop exactly.'
        )
        tags = ['image', 'crop', 'xmp', 'deskew', 'autocrop', 'tekst']

    UNCROPPED_PATH = PropertyDescriptor(
        name='Uncropped Image Path',
        description='Absolute path to the uncropped source image.',
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
    )
    CROPPED_PATH = PropertyDescriptor(
        name='Cropped Image Path',
        description='Absolute path to the cropped image to locate within the uncropped image.',
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
    )
    RESIZE_FACTOR = PropertyDescriptor(
        name='Resize Factor',
        description='Downsample factor before template matching. Lower = faster, less precise. Typical: 0.1.',
        required=True,
        default_value='0.1',
        validators=[StandardValidators.NUMBER_VALIDATOR],
    )
    CHECK_INVERTED = PropertyDescriptor(
        name='Check Inverted',
        description='Also try colour-inverted template matching for images with inverted tones.',
        required=True,
        allowable_values=['true', 'false'],
        default_value='true',
    )
    ALTO_PATH = PropertyDescriptor(
        name='Alto XML Path',
        description=(
            'Optional path to the ALTO XML file for the cropped image. '
            'When set, the processor validates that the detected crop dimensions '
            'match the ALTO page dimensions within the configured tolerance. '
            'Routes to dimension_mismatch if they differ by more than the tolerance.'
        ),
        required=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
    )
    DIMENSION_TOLERANCE = PropertyDescriptor(
        name='Dimension Tolerance',
        description=(
            'Maximum allowed pixel difference between detected crop dimensions '
            'and ALTO page dimensions. Only used when Alto XML Path is set.'
        ),
        required=True,
        default_value='200',
        validators=[StandardValidators.NUMBER_VALIDATOR],
    )
    USE_HOUGH = PropertyDescriptor(
        name='Use Hough Estimation',
        description=(
            'Use Hough line detection to estimate the document skew angle before '
            'running the full rotation search. When confident, this seeds a narrow '
            '±2° search window instead of scanning the full ±5° grid, improving '
            'speed and extending effective range. Falls back to the full grid if '
            'Hough is not confident or the seeded match score is too low.'
        ),
        required=True,
        allowable_values=['true', 'false'],
        default_value='true',
    )

    def __init__(self, **kwargs):
        super().__init__()

    property_descriptors = [
        UNCROPPED_PATH,
        CROPPED_PATH,
        RESIZE_FACTOR,
        CHECK_INVERTED,
        ALTO_PATH,
        DIMENSION_TOLERANCE,
        USE_HOUGH,
    ]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def transform(self, context, flowfile):
        try:
            uncropped_path = (
                context.getProperty(self.UNCROPPED_PATH.name)
                .evaluateAttributeExpressions(flowfile).getValue()
            )
            cropped_path = (
                context.getProperty(self.CROPPED_PATH.name)
                .evaluateAttributeExpressions(flowfile).getValue()
            )
            resize_factor = float(context.getProperty(self.RESIZE_FACTOR.name).getValue())
            check_inverted = (
                context.getProperty(self.CHECK_INVERTED.name).getValue().lower() == 'true'
            )
            use_hough = (
                context.getProperty(self.USE_HOUGH.name).getValue().lower() == 'true'
            )

            uncropped = read_image(uncropped_path)
            if uncropped is None:
                self.logger.error(f'Cannot read uncropped image: {uncropped_path}')
                return FlowFileTransformResult(relationship='failure')

            cropped = read_image(cropped_path)
            if cropped is None:
                self.logger.error(f'Cannot read cropped image: {cropped_path}')
                return FlowFileTransformResult(relationship='failure')

            t0 = time.perf_counter()
            result = find_crop(
                uncropped, cropped,
                resize_factor=resize_factor,
                check_inverted=check_inverted,
                use_hough=use_hough,
            )
            elapsed_ms = round((time.perf_counter() - t0) * 1000)

            img_h, img_w = uncropped.shape[:2]
            top, left, bottom, right = transform_corners_to_deskewed(
                result.corners, result.rotation, img_w, img_h,
            )
            top, left, bottom, right = snap_bounds_to_known_size(
                top, left, bottom, right, cropped.shape[1], cropped.shape[0],
            )
            xmp = build_xmp(result.rotation, top, left, bottom, right)

            tolerance = int(float(context.getProperty(self.DIMENSION_TOLERANCE.name).getValue()))
            attributes = {
                'crop.score':              str(round(float(result.score),    6)),
                'crop.rotation':           str(round(float(result.rotation), 6)),
                'crop.used_hough':         str(result.used_hough).lower(),
                'crop.resize_factor':      str(resize_factor),
                'crop.check_inverted':     str(check_inverted).lower(),
                'crop.dimension_tolerance': str(tolerance),
                'crop.elapsed_ms':         str(elapsed_ms),
            }

            alto_path = (
                context.getProperty(self.ALTO_PATH.name)
                .evaluateAttributeExpressions(flowfile).getValue()
            )
            if alto_path:
                try:
                    with open(alto_path, 'r', encoding='utf-8') as f:
                        alto_xml = f.read()
                    alto_w_mm10, alto_h_mm10 = parse_alto_page_dimensions(alto_xml)

                    expected_w = alto_to_pixels(alto_w_mm10, NB_ACCESS_SCALE)
                    expected_h = alto_to_pixels(alto_h_mm10, NB_ACCESS_SCALE)
                    found_w = right - left
                    found_h = bottom - top
                    error_w = abs(found_w - expected_w)
                    error_h = abs(found_h - expected_h)
                    match = error_w <= tolerance and error_h <= tolerance

                    attributes['crop.dimension_match']   = str(match).lower()
                    attributes['crop.dimension_error_w'] = str(error_w)
                    attributes['crop.dimension_error_h'] = str(error_h)

                    if not match:
                        self.logger.error(
                            f'Crop dimension mismatch: found {found_w}x{found_h}, '
                            f'expected {expected_w}x{expected_h} (tolerance {tolerance}px)'
                        )
                        return FlowFileTransformResult(
                            relationship='dimension_mismatch',
                            contents=xmp.encode('utf-8'),
                            attributes=attributes,
                        )
                except Exception as e:
                    self.logger.error(f'ALTO validation failed: {e}')
                    return FlowFileTransformResult(relationship='failure')

            return FlowFileTransformResult(
                relationship='success',
                contents=xmp.encode('utf-8'),
                attributes=attributes,
            )
        except Exception as e:
            self.logger.error(f'FindCrop failed: {e}')
            return FlowFileTransformResult(relationship='failure')
