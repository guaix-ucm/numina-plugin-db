#
# Copyright 2017-2025 Universidad Complutense de Madrid
#
# This file is part of Numina DB
#
# SPDX-License-Identifier: GPL-3.0-or-later
# License-Filename: LICENSE.txt
#
"""Base class for database support"""


from sqlalchemy.orm import declarative_base

Base = declarative_base()
