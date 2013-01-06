#ifndef SRC_BITMASK_H
#define SRC_BITMASK_H

struct Mask {
  public:
    Mask() : mask_(0) {}
    ~Mask() {}

    bool is_set(const NodeState& state) {
        uint32_t a = 1 << state;
        bool ret = a & mask_;
        return (ret != 0);
    }

    void clear() {
        mask_ = 0;
    }

    void set(const NodeState& state) {
        uint32_t a = 1 << state;
        mask_ = a | mask_;
    }

    void unset(const NodeState& state) {
        uint32_t a = 0xffffffff - (1 << state);
        mask_ = a & mask_;
    }

    bool zero() {
        return (mask_ == 0);
    }

    uint32_t and_mask(uint32_t m) {
        return mask_ & m;
    }

    uint32_t or_mask(uint32_t m) {
        return mask_ | m;
    }
  private:
    uint32_t mask_;
};


#endif // SRC_BITMASK_H
