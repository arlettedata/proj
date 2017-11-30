/* *      Streaming XML Parser.
 *
 *      Copyright (c) 2005-2017 by Brian. Kramer
 *
 * This software is provided 'as-is', without any express or implied
 *
 * warranty.  In no event will the authors be held liable for any damages
 * arising from the use of this software.
 *
 * Permission is granted to anyone to use this software for any purpose,
 * including commercial applications, and to alter it and redistribute it
 * freely, subject to the following restrictions (known as zlib license):
 *
 * 1. The origin of this software must not be misrepresented; you must not
 *    claim that you wrote the original software. If you use this software
 *    in a product, an acknowledgment in the product documentation would be
 *    appreciated but is not required.
 * 2. Altered source versions must be plainly marked as such, and must not be
 *    misrepresented as being the original software.
 * 3. This notice may not be removed or altered from any source distribution.
 */

#pragma once

#include <algorithm>
#include <float.h>
#include <iomanip>
#include <map>
#include <math.h>
#include <string>
#include <vector>

#include "xmlbase.h"

namespace StreamingXml
{

enum XmlAggrType
{
    Any,
    Count,
    Min,
    Max,
    Sum,
    Avg,
    Stdev,
    Var,
    Cov,
    Corr
};

class XmlAggregate
{
public:
    void UpdateAny(const XmlValue& value)
    {
        if ((m_any.type == XmlType::Unknown || m_any.type == XmlType::String) && m_any.sval.size() == 0) {
            m_any = value;
        }
    }

    void Update(double value)
    {
        m_v1.Update(value);
    }

    void Update(double value1, double value2)
    {
        m_v1.Update(value1);
        m_v2.Update(value2);
        m_cov.Update(value1, value2);
    }

    XmlValue GetAggregate(XmlAggrType type) const
    {
        switch (type) {
            case Any:
                return m_any;

            case Count:
                return (double)m_v1.m_count;
                break;

            case Min:
                return m_v1.m_min;
                break;

            case Max:
                return m_v1.m_max;
                break;

            case Sum:
                return m_v1.m_sum;
                break;

            case Avg:
                return m_v1.m_sum / m_v1.m_count;
                break;

            case Stdev:
                return (m_v1.m_count < 2)
                    ? 0.0
                    : sqrt((m_v1.m_sum_sq - (m_v1.m_sum * m_v1.m_sum) / m_v1.m_count) / (m_v1.m_count - 1));
                break;

            case Var:
                return (m_v1.m_count < 2)
                    ? 0.0
                    : (m_v1.m_sum_sq - (m_v1.m_sum * m_v1.m_sum) / m_v1.m_count) / (m_v1.m_count - 1);
                break;

            case Cov:
                return m_cov.GetCovariance();
                break;

            case Corr:
                return m_cov.GetCorrelation();
                break;

            default:
                assert(!"unreached");
                return 0.0;
        }
    }

private:
    class BasicAggrHelper
    {
    public:
        BasicAggrHelper()
        {
            m_count = 0;
            m_min = DBL_MAX;
            m_max = DBL_MIN;
            m_sum = 0.0;
            m_sum_sq = 0.0;
        }

        void Update(double x)
        {
            m_count++;
            if (x < m_min) {
                m_min = x;
            }
            if (x > m_max) {
                m_max = x;
            }
            m_sum += x;
            m_sum_sq += x * x;
        }

        int m_count;
        double m_min;
        double m_max;
        double m_sum;
        double m_sum_sq;
    };

    class CovarianceHelper
    {
    public:
        CovarianceHelper()
        {
            m_count = 0;
            m_sum_sq_x = 0;
            m_sum_sq_y = 0;
            m_sum_coproduct = 0;
            m_mean_x = 0;
            m_mean_y = 0;
        }

        void Update(double x, double y)
        {
            // Correlation math adapted from single-pass algorithm described in Wikipedia
            m_count++;
            if (m_count == 1) {
                m_mean_x = x;
                m_mean_y = y;
            }
            else {
                double rescale = (double)(m_count - 1) / m_count;
                double delta_x = x - m_mean_x;
                double delta_y = y - m_mean_y;
                m_sum_sq_x += delta_x * delta_x * rescale;
                m_sum_sq_y += delta_y * delta_y * rescale;
                m_sum_coproduct += delta_x * delta_y * rescale;
                m_mean_x += delta_x / m_count;
                m_mean_y += delta_y / m_count;
            }
        }

        double GetCovariance() const
        {
            if (!m_count) {
                return 0;
            }
            return m_sum_coproduct / m_count;
        }

        double GetCorrelation() const
        {
            if (!m_count) {
                return 0;
            }
            double pop_sd_x = sqrt(m_sum_sq_x / m_count);
            double pop_sd_y = sqrt(m_sum_sq_y / m_count);
            return GetCovariance() / (pop_sd_x * pop_sd_y);
        }

        int m_count;
        double m_sum_sq_x;
        double m_sum_sq_y;
        double m_sum_coproduct;
        double m_mean_x;
        double m_mean_y;
    };

    XmlValue m_any; // used for any() function
    BasicAggrHelper m_v1;
    BasicAggrHelper m_v2;
    CovarianceHelper m_cov;
};

} // namespace StreamingXml
