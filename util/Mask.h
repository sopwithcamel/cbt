#ifndef SRC_BITMASK_H
#define SRC_BITMASK_H

struct Mask {
  public:
    Mask() : mask_(0) {}
    ~Mask() {}

    bool is_set(uint32_t t) {
        uint32_t a = 1 << t;
        bool ret = a & mask_;
        return (ret != 0);
    }

    void clear() {
        mask_ = 0;
    }

    void set(uint32_t t) {
        uint32_t a = 1 << t;
        mask_ = a | mask_;
    }

    void unset(uint32_t t) {
        uint32_t a = 0xffffffff - (1 << t);
        mask_ = a & mask_;
    }

    bool zero() {
        return (mask_ == 0);
    }

    Mask and_mask(const Mask& m) {
        Mask r;
        r. mask_ = mask_ & m.mask_;
        return r;
    }

    Mask or_mask(const Mask& m) {
        Mask r;
        r.mask_ = mask_ | m.mask_;
        return r;
    }

    bool operator==(const Mask& rhs) {
        if (mask_ == rhs.mask_)
            return true;
        return false;
    }

    uint32_t mask_;
};


#endif // SRC_BITMASK_H
